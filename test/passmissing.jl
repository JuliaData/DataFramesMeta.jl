module Testeachrow

using Test
using DataFrames
using DataFramesMeta
using Statistics

const ≅ = isequal

@testset "passmissing by row" begin

    no_missing(x::Int, y::Int) = x + y

    df = DataFrame(a = [1, 2, missing], b = [4, 5, 6])

    d = @transform df @byrow @passmissing :c = no_missing(:a, :b)
    @test d.c ≅ [5, 7, missing]

    d = @transform df @passmissing @byrow :c = no_missing(:a, :b)
    @test d.c ≅ [5, 7, missing]

    d = @transform df @byrow begin
        :c = :a + :b
        @passmissing :d = no_missing(:a, :b)
    end
    @test d.c ≅ d.d

    d = @rselect df @passmissing :c = no_missing(:a, :b)
    @test d.c ≅ [5, 7, missing]

    d = @rselect df @passmissing @byrow :c = no_missing(:a, :b)
    @test d.c ≅ [5, 7, missing]

    d = @rselect df @byrow begin
        :c = :a + :b
        @passmissing :d = no_missing(:a, :b)
    end
    @test d.c ≅ d.d

    d = @rorderby df @passmissing no_missing(:a, :b)
    @test d ≅ df
end

end # module
