package mqtt
import (
	"testing"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"time"
	"strconv"
)

func TestRetainInLeveldb(t *testing.T) {
	db, err := leveldb.OpenFile("test.db", nil)
	assert.NoError(t, err, "failed to open level.db")

	start := time.Now()
	for key, _ := range make([]byte, 100000) {
		db.Put([]byte("a/b/" + strconv.Itoa(key)), []byte("message"), nil)
	}
	log.Println(float64(100000) / time.Since(start).Seconds())


	start = time.Now()
	for _ = range make([]byte, 500000) {
		key := rand.Intn(10000)
//		strconv.Itoa(key)
//		db.Get([]byte("a/b/" + strconv.Itoa(key)), nil)
		value, err := db.Get([]byte("a/b/" + strconv.Itoa(key)), nil)
		assert.NoError(t, err)
		assert.Equal(t, value, []byte("message"))
	}

	log.Println(float64(500000) / time.Since(start).Seconds())
}
