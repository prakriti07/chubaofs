package datanode

import (
	"context"
	"fmt"
	"golang.org/x/time/rate"
	"testing"
	"time"
)

func init() {
	autoRepairLimiteRater = rate.NewLimiter(rate.Inf, 512)
}

func TestAutoRepairLimiterWait(t *testing.T) {
	ctx := context.Background()
	ctx, _ = context.WithTimeout(ctx, time.Second*10)
	autoRepairLimiteRater.SetLimit(1)
	for {
		err := autoRepairLimiteRater.Wait(ctx)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("hahahah")
	}

}
