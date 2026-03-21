package pipeline

import (
	"context"

	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

func StageSendData(
	ctx context.Context,
	cfg *Config,
	memRaw *MemSemaphore,
	win *WindowSemaphore,
	stream grpc.ClientStreamingClient[apiv1.SyncRequest, apiv1.SyncResponse],
	inCh <-chan CompressedChunk,
) error {
	return dataSendWorker(ctx, cfg, memRaw, win, stream, inCh)
}

func dataSendWorker(
	ctx context.Context,
	cfg *Config,
	memRaw *MemSemaphore,
	win *WindowSemaphore,
	stream grpc.ClientStreamingClient[apiv1.SyncRequest, apiv1.SyncResponse],
	inCh <-chan CompressedChunk,
) error {
	var (
		blocks  []*apiv1.ChangedBlock
		pending []held
		accum   int
		curPath string
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

	for {
		var cc CompressedChunk
		var ok bool
		select {
		case cc, ok = <-inCh:
			if !ok {
				return flush()
			}
		case <-ctx.Done():
			for i := range pending {
				pending[i].release(memRaw, nil, win)
			}
			return ctx.Err()
		}

		curPath = cc.FilePath

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
