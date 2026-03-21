package pipeline

import (
	"context"
	"fmt"
	"runtime"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

type pendingCommit struct {
	path      string
	lastReqID uint64
}

func sendCommit(
	stream grpc.ClientStreamingClient[apiv1.SyncRequest, apiv1.SyncResponse],
	path string,
) error {
	return stream.Send(&apiv1.SyncRequest{
		Operation: &apiv1.SyncRequest_Commit{
			Commit: &apiv1.CommitRequest{
				Path: path,
			},
		},
	})
}

func StageSendData(
	ctx context.Context,
	cfg *Config,
	memRaw *MemSemaphore,
	win *WindowSemaphore,
	newStream StreamFactory,
	inCh <-chan CompressedChunk,
	reader DataReader,
) error {
	g, gctx := errgroup.WithContext(ctx)
	for range cfg.DataSendWorkers {
		g.Go(func() error {
			stream, err := newStream(gctx)
			if err != nil {
				return fmt.Errorf(
					"open sync stream: %w", err,
				)
			}
			return dataSendWorker(
				gctx, cfg, memRaw, win,
				stream, inCh, reader,
			)
		})
	}
	return g.Wait()
}

func dataSendWorker(
	ctx context.Context,
	cfg *Config,
	memRaw *MemSemaphore,
	win *WindowSemaphore,
	stream grpc.ClientStreamingClient[apiv1.SyncRequest, apiv1.SyncResponse],
	inCh <-chan CompressedChunk,
	reader DataReader,
) error {
	var (
		blocks    []*apiv1.ChangedBlock
		pending   []held
		accum     int
		curPath   string
		prevReqID uint64
		commits   []pendingCommit
	)

	flush := func() error {
		if len(blocks) == 0 {
			return nil
		}

		req := &apiv1.SyncRequest{
			Operation: &apiv1.SyncRequest_Write{
				Write: &apiv1.WriteRequest{
					Path:   curPath,
					Blocks: blocks,
				},
			},
		}

		if err := stream.Send(req); err != nil {
			for i := range pending {
				pending[i].release(memRaw, nil, win)
			}
			return err
		}

		for i := range pending {
			pending[i].release(memRaw, nil, win)
		}

		blocks = nil
		pending = nil
		accum = 0
		return nil
	}

	drainPending := func(currentReqID uint64) error {
		for len(commits) > 0 {
			head := commits[0]
			if currentReqID < head.lastReqID+uint64(cfg.MaxWindow)+2 {
				break
			}
			if err := sendCommit(stream, head.path); err != nil {
				return err
			}
			if err := reader.CloseFile(head.path); err != nil {
				return err
			}
			commits = commits[1:]
		}
		return nil
	}

	for {
		var cc CompressedChunk
		var ok bool
		select {
		case cc, ok = <-inCh:
			if !ok {
				if err := flush(); err != nil {
					return err
				}

				// Drain remaining commits using IsReleased
				for len(commits) > 0 {
					head := commits[0]
					for !win.IsReleased(head.lastReqID) {
						runtime.Gosched()
					}
					if err := sendCommit(stream, head.path); err != nil {
						return err
					}
					if err := reader.CloseFile(head.path); err != nil {
						return err
					}
					commits = commits[1:]
				}

				// Send commit for final file
				if curPath != "" {
					if err := sendCommit(stream, curPath); err != nil {
						return err
					}
					if err := reader.CloseFile(curPath); err != nil {
						return err
					}
				}

				if _, err := stream.CloseAndRecv(); err != nil {
					return fmt.Errorf(
						"close sync stream: %w", err,
					)
				}
				return nil
			}
		case <-ctx.Done():
			for i := range pending {
				pending[i].release(memRaw, nil, win)
			}
			return ctx.Err()
		}

		// File boundary detected
		if curPath != "" && cc.FilePath != curPath {
			if err := flush(); err != nil {
				return err
			}
			commits = append(commits, pendingCommit{
				path:      curPath,
				lastReqID: prevReqID,
			})
			if err := drainPending(cc.ReqID); err != nil {
				return err
			}
		}

		curPath = cc.FilePath
		prevReqID = cc.ReqID

		algo := apiv1.CompressionAlgo_COMPRESSION_LZ4
		if cc.IsRaw {
			algo = apiv1.CompressionAlgo_COMPRESSION_NONE
		}

		block := &apiv1.ChangedBlock{
			Offset:      uint64(cc.Offset),
			Length:      uint64(cc.UncompressedLength),
			Data:        cc.Data,
			RequestId:   cc.ReqID,
			Compression: algo,
		}

		blocks = append(blocks, block)
		pending = append(pending, cc.Held)
		accum += len(cc.Data)

		if accum >= int(cfg.DataBatchMaxBytes) || len(blocks) >= cfg.DataBatchMaxCount {
			if err := flush(); err != nil {
				return err
			}
		}
	}
}
