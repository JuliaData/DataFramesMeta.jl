module TestFunctionCompilation

using Test
using DataFrames
using DataFramesMeta
using Statistics

@testset "function_compilation" begin
	df = DataFrame(a = [1], b = [2])

	testfun(x, y) = x .* y
	testdotfun(x, y) = x * y

	@test @select(df, c = :a .+ :b) == DataFrame(c = [3])
	@test @select(df, )

end
end # module