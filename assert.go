package iceberg

import "log"

func Assert(predicate bool, assertFailMsg string) {
	if !predicate {
		log.Fatal(assertFailMsg)
	}
}
