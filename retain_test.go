package mqtt
import (
	"testing"
	"strconv"
)


func BenchmarkStringConcat(b *testing.B) {
	for i := 0; i < b.N; i++ {
//		fmt.Sprintf("packets:in:%v:%v", "12312341234", 123)
		_ = "packets:in:" + "12312341234" + ":" + strconv.Itoa(123)
	}
}
