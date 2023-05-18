# momento-go-ddtrace

Purpose:

Wrap the Momento Client with custom Datadog tracing. Each operation will get a separate Span attached to the trace based on the context supplied. The operation name will be set as well based upon the Method call

### Usage

```golang
import (
	"github.com/benbpyle/momento-go-ddtrace"
)

func NewMomentoClient(token string) (momento.CacheClient, error) {
	// Initializes Momento
	return momentoddtrace.NewWrappedCacheClient(token)
}
```
