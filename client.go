package momentoddtrace

import (
	"context"
	"time"

	"github.com/momentohq/client-sdk-go/auth"
	"github.com/momentohq/client-sdk-go/config"
	"github.com/momentohq/client-sdk-go/momento"
	"github.com/momentohq/client-sdk-go/responses"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

var opts = []ddtrace.StartSpanOption{
	tracer.ServiceName("momento"),
}

type WrappedCacheClient struct {
	client momento.CacheClient
}

func NewWrappedCacheClient(token string) (*WrappedCacheClient, error) {
	credentialProvider, err := auth.FromString(token)

	if err != nil {
		return nil, err
	}

	// Initializes Momento
	client, err := momento.NewCacheClient(
		config.LaptopLatest(),
		credentialProvider,
		600*time.Second)

	if err != nil {
		return nil, err
	}

	c := &WrappedCacheClient{
		client: client,
	}

	return c, nil
}

func (w *WrappedCacheClient) CreateCache(ctx context.Context, request *momento.CreateCacheRequest) (responses.CreateCacheResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.CreateCache", opts...)
	defer span.Finish()
	return w.client.CreateCache(ctx, request)
}

func (w *WrappedCacheClient) DeleteCache(ctx context.Context, request *momento.DeleteCacheRequest) (responses.DeleteCacheResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.DeleteCache", opts...)
	defer span.Finish()
	return w.client.DeleteCache(ctx, request)
}

func (w *WrappedCacheClient) ListCaches(ctx context.Context, request *momento.ListCachesRequest) (responses.ListCachesResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.ListCaches", opts...)
	defer span.Finish()
	return w.client.ListCaches(ctx, request)
}

func (w *WrappedCacheClient) Set(ctx context.Context, r *momento.SetRequest) (responses.SetResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.Set", opts...)
	defer span.Finish()
	return w.client.Set(ctx, r)
}

func (w *WrappedCacheClient) Get(ctx context.Context, r *momento.GetRequest) (responses.GetResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.Get", opts...)
	defer span.Finish()
	return w.client.Get(ctx, r)
}

func (w *WrappedCacheClient) Delete(ctx context.Context, r *momento.DeleteRequest) (responses.DeleteResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.Delete", opts...)
	defer span.Finish()
	return w.client.Delete(ctx, r)
}

func (w *WrappedCacheClient) KeysExist(ctx context.Context, r *momento.KeysExistRequest) (responses.KeysExistResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.KeysExist", opts...)
	defer span.Finish()
	return w.client.KeysExist(ctx, r)
}

func (w *WrappedCacheClient) SortedSetFetchByRank(ctx context.Context, r *momento.SortedSetFetchByRankRequest) (responses.SortedSetFetchResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.SortedSetFetchByRank", opts...)
	defer span.Finish()
	return w.client.SortedSetFetchByRank(ctx, r)
}

func (w *WrappedCacheClient) SortedSetFetchByScore(ctx context.Context, r *momento.SortedSetFetchByScoreRequest) (responses.SortedSetFetchResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.SortedSetFetchByScore", opts...)
	defer span.Finish()
	return w.client.SortedSetFetchByScore(ctx, r)
}

func (w *WrappedCacheClient) SortedSetPutElement(ctx context.Context, r *momento.SortedSetPutElementRequest) (responses.SortedSetPutElementResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.SortedSetPutElement", opts...)
	defer span.Finish()
	return w.client.SortedSetPutElement(ctx, r)
}

func (w *WrappedCacheClient) SortedSetPutElements(ctx context.Context, r *momento.SortedSetPutElementsRequest) (responses.SortedSetPutElementsResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.SortedSetPutElements", opts...)
	defer span.Finish()
	return w.client.SortedSetPutElements(ctx, r)
}

func (w *WrappedCacheClient) SortedSetGetScores(ctx context.Context, r *momento.SortedSetGetScoresRequest) (responses.SortedSetGetScoresResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.SortedSetGetScores", opts...)
	defer span.Finish()
	return w.client.SortedSetGetScores(ctx, r)
}

func (w *WrappedCacheClient) SortedSetGetScore(ctx context.Context, r *momento.SortedSetGetScoreRequest) (responses.SortedSetGetScoreResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.SortedSetGetScore", opts...)
	defer span.Finish()
	return w.client.SortedSetGetScore(ctx, r)
}

func (w *WrappedCacheClient) SortedSetRemoveElement(ctx context.Context, r *momento.SortedSetRemoveElementRequest) (responses.SortedSetRemoveElementResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.SortedSetRemoveElement", opts...)
	defer span.Finish()
	return w.client.SortedSetRemoveElement(ctx, r)
}

func (w *WrappedCacheClient) SortedSetRemoveElements(ctx context.Context, r *momento.SortedSetRemoveElementsRequest) (responses.SortedSetRemoveElementsResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.SortedSetRemoveElements", opts...)
	defer span.Finish()
	return w.client.SortedSetRemoveElements(ctx, r)
}

func (w *WrappedCacheClient) SortedSetGetRank(ctx context.Context, r *momento.SortedSetGetRankRequest) (responses.SortedSetGetRankResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.SortedSetGetRank", opts...)
	defer span.Finish()
	return w.client.SortedSetGetRank(ctx, r)
}

func (w *WrappedCacheClient) SortedSetIncrementScore(ctx context.Context, r *momento.SortedSetIncrementScoreRequest) (responses.SortedSetIncrementScoreResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.SortedSetIncrementScore", opts...)
	defer span.Finish()
	return w.client.SortedSetIncrementScore(ctx, r)
}

func (w *WrappedCacheClient) SetAddElement(ctx context.Context, r *momento.SetAddElementRequest) (responses.SetAddElementResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.SetAddElement", opts...)
	defer span.Finish()
	return w.client.SetAddElement(ctx, r)
}

func (w *WrappedCacheClient) SetAddElements(ctx context.Context, r *momento.SetAddElementsRequest) (responses.SetAddElementsResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.SetAddElements", opts...)
	defer span.Finish()
	return w.client.SetAddElements(ctx, r)
}

func (w *WrappedCacheClient) SetFetch(ctx context.Context, r *momento.SetFetchRequest) (responses.SetFetchResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.SetFetch", opts...)
	defer span.Finish()
	return w.client.SetFetch(ctx, r)
}

func (w *WrappedCacheClient) SetRemoveElement(ctx context.Context, r *momento.SetRemoveElementRequest) (responses.SetRemoveElementResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.SetRemoveElement", opts...)
	defer span.Finish()
	return w.client.SetRemoveElement(ctx, r)
}

func (w *WrappedCacheClient) SetRemoveElements(ctx context.Context, r *momento.SetRemoveElementsRequest) (responses.SetRemoveElementsResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.SetRemoveElements", opts...)
	defer span.Finish()
	return w.client.SetRemoveElements(ctx, r)
}

func (w *WrappedCacheClient) SetContainsElements(ctx context.Context, r *momento.SetContainsElementsRequest) (responses.SetContainsElementsResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.SetContainsElements", opts...)
	defer span.Finish()
	return w.client.SetContainsElements(ctx, r)
}

func (w *WrappedCacheClient) ListPushFront(ctx context.Context, r *momento.ListPushFrontRequest) (responses.ListPushFrontResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.ListPushFront", opts...)
	defer span.Finish()
	return w.client.ListPushFront(ctx, r)
}

func (w *WrappedCacheClient) ListPushBack(ctx context.Context, r *momento.ListPushBackRequest) (responses.ListPushBackResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.ListPushBack", opts...)
	defer span.Finish()
	return w.client.ListPushBack(ctx, r)
}

func (w *WrappedCacheClient) ListPopFront(ctx context.Context, r *momento.ListPopFrontRequest) (responses.ListPopFrontResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.ListPopFront", opts...)
	defer span.Finish()
	return w.client.ListPopFront(ctx, r)
}

func (w *WrappedCacheClient) ListPopBack(ctx context.Context, r *momento.ListPopBackRequest) (responses.ListPopBackResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.ListPopBack", opts...)
	defer span.Finish()
	return w.client.ListPopBack(ctx, r)
}

func (w *WrappedCacheClient) ListConcatenateFront(ctx context.Context, r *momento.ListConcatenateFrontRequest) (responses.ListConcatenateFrontResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.ListConcatenateFront", opts...)
	defer span.Finish()
	return w.client.ListConcatenateFront(ctx, r)
}

func (w *WrappedCacheClient) ListConcatenateBack(ctx context.Context, r *momento.ListConcatenateBackRequest) (responses.ListConcatenateBackResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.ListConcatenateBack", opts...)
	defer span.Finish()
	return w.client.ListConcatenateBack(ctx, r)
}

func (w *WrappedCacheClient) ListFetch(ctx context.Context, r *momento.ListFetchRequest) (responses.ListFetchResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.ListFetch", opts...)
	defer span.Finish()
	return w.client.ListFetch(ctx, r)
}

func (w *WrappedCacheClient) ListLength(ctx context.Context, r *momento.ListLengthRequest) (responses.ListLengthResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.ListLength", opts...)
	defer span.Finish()
	return w.client.ListLength(ctx, r)
}

func (w *WrappedCacheClient) ListRemoveValue(ctx context.Context, r *momento.ListRemoveValueRequest) (responses.ListRemoveValueResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.ListRemoveValue", opts...)
	defer span.Finish()
	return w.client.ListRemoveValue(ctx, r)
}

func (w *WrappedCacheClient) DictionarySetField(ctx context.Context, r *momento.DictionarySetFieldRequest) (responses.DictionarySetFieldResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.DictionarySetField", opts...)
	defer span.Finish()
	return w.client.DictionarySetField(ctx, r)
}

func (w *WrappedCacheClient) DictionarySetFields(ctx context.Context, r *momento.DictionarySetFieldsRequest) (responses.DictionarySetFieldsResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.DictionarySetFields", opts...)
	defer span.Finish()
	return w.client.DictionarySetFields(ctx, r)
}

func (w *WrappedCacheClient) DictionaryFetch(ctx context.Context, r *momento.DictionaryFetchRequest) (responses.DictionaryFetchResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.DictionaryFetch", opts...)
	defer span.Finish()
	return w.client.DictionaryFetch(ctx, r)
}

func (w *WrappedCacheClient) DictionaryGetField(ctx context.Context, r *momento.DictionaryGetFieldRequest) (responses.DictionaryGetFieldResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.DictionaryGetField", opts...)
	defer span.Finish()
	return w.client.DictionaryGetField(ctx, r)
}

func (w *WrappedCacheClient) DictionaryGetFields(ctx context.Context, r *momento.DictionaryGetFieldsRequest) (responses.DictionaryGetFieldsResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.DictionaryGetFields", opts...)
	defer span.Finish()
	return w.client.DictionaryGetFields(ctx, r)
}

func (w *WrappedCacheClient) DictionaryIncrement(ctx context.Context, r *momento.DictionaryIncrementRequest) (responses.DictionaryIncrementResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.DictionaryIncrement", opts...)
	defer span.Finish()
	return w.client.DictionaryIncrement(ctx, r)
}

func (w *WrappedCacheClient) DictionaryRemoveField(ctx context.Context, r *momento.DictionaryRemoveFieldRequest) (responses.DictionaryRemoveFieldResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.DictionaryRemoveField", opts...)
	defer span.Finish()
	return w.client.DictionaryRemoveField(ctx, r)
}

func (w *WrappedCacheClient) DictionaryRemoveFields(ctx context.Context, r *momento.DictionaryRemoveFieldsRequest) (responses.DictionaryRemoveFieldsResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.DictionaryRemoveFields", opts...)
	defer span.Finish()
	return w.client.DictionaryRemoveFields(ctx, r)
}

func (w *WrappedCacheClient) UpdateTtl(ctx context.Context, r *momento.UpdateTtlRequest) (responses.UpdateTtlResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.UpdateTtl", opts...)
	defer span.Finish()
	return w.client.UpdateTtl(ctx, r)
}

func (w *WrappedCacheClient) IncreaseTtl(ctx context.Context, r *momento.IncreaseTtlRequest) (responses.IncreaseTtlResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.IncreaseTtl", opts...)
	defer span.Finish()
	return w.client.IncreaseTtl(ctx, r)
}

func (w *WrappedCacheClient) DecreaseTtl(ctx context.Context, r *momento.DecreaseTtlRequest) (responses.DecreaseTtlResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.DecreaseTtl", opts...)
	defer span.Finish()
	return w.client.DecreaseTtl(ctx, r)
}

func (w *WrappedCacheClient) Ping(ctx context.Context) (responses.PingResponse, error) {
	span, _ := tracer.StartSpanFromContext(ctx, "momento.Ping", opts...)
	defer span.Finish()
	return w.client.Ping(ctx)
}

func (w *WrappedCacheClient) Close() {
	w.client.Close()
}
