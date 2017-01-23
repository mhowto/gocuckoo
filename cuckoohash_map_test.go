package cuckoo

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func TestAltIndex(t *testing.T) {
	Convey("alt_index return the first possible bucket if index is the second possible bucket", t, func() {
		hv := hashedKey("ucloud")
		i1 := indexHash(16, hv.hash)
		i2 := altIndex(16, hv.partial, i1)
		i3 := altIndex(16, hv.partial, i2)

		So(i1, ShouldEqual, i3)

		rand.Seed(time.Now().Unix())
		for i := 0; i < 20; i++ {
			h := uint64(rand.Intn(9999999999))
			partial := partialKey(h)
			fmt.Println("h", h, "partial", partial)
			i1 := indexHash(16, h)
			i2 := altIndex(16, partial, i1)
			i3 := altIndex(16, partial, i2)
			So(i1, ShouldEqual, i3)
		}
	})
}
