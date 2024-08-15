package basemath

import "math"

func AbsInt(x int) int {
	return AbsDiffInt(x, 0)
}

func AbsDiffInt(x, y int) int {
	if x < y {
		return y - x
	}
	return x - y
}

func AbsDiffUint(x, y uint) uint {
	if x < y {
		return y - x
	}

	return x - y
}

func IntMin(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func IntMax(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func UintMin(x, y uint) uint {
	if x < y {
		return x
	}
	return y
}

func UintMax(x, y uint) uint {
	if x > y {
		return x
	}
	return y
}

func LnFactorial(n int) float64 {
	// for property 0! = 1 since exp(0) == 1
	if n == 0 {
		return 0
	}

	var ret float64 = 0

	for i := range n {
		ret += math.Log(float64(i + 1))
	}

	return ret
}

func Factorial(n int) uint64 {
	if n == 0 {
		return 1
	}

	return uint64(math.Round(math.Exp(LnFactorial(n))))
}
