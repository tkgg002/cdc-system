package main

import (
	"fmt"
	"centralized-data-service/internal/naming"
)

func main() {
	fmt.Println("Normalized:", naming.NormalizeIdentifier("refund-requests"))
}
