module Testbyrow

using Test
using DataFrames
using DataFramesMeta
using Statistics

const ≅ = isequal


@testset "AsTable on RHS" begin
    df = DataFrame(a = [1, 2], b = [3, 4], d = [100, 200])

    res = DataFrame(a = [1, 2], b = [3, 4], d = [100, 200], c = [4, 6])

    d = @transform df :c = sum(AsTable([:a, :b]))
    @test d == res

    d = @rtransform df :c = sum(AsTable([:a, :b]))
    @test d == res

    d = @transform df @byrow :c = sum(AsTable([:a, :b]))
    @test d == res

    vars = ["a", "b"]

    d = @rtransform df :c = sum(AsTable(vars))
    @test d == res

    d = @rtransform df :c = sum(AsTable(Between(:a, :b)))
    @test d == res

    d = @transform df :c = sum(AsTable(Not(:d)))
    @test d == res
end

end # module