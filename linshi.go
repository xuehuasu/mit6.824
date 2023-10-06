package main

import (
	"fmt"
)

func main() {
	vec := make([]int, 0)
	vec = append(vec, 1)
	vec = append(vec, 2)
	vec = append(vec, 3)
	vec = append(vec, 4)
	vec = append(vec, 5)

	fmt.Println(vec)
	fmt.Println(len(vec))

	vec = vec[:2]

	fmt.Println(vec)
	fmt.Println(len(vec))

}
