module TestAsTableFlag

using Test
using DataFrames
using DataFramesMeta
using Statistics

const ≅ = isequal

@testset "@astable macro flag" begin
    df = DataFrame(a = 1, b = 2)

    d = @rtransform df @astable begin
        :x = 1
        y = 50
        :a = :x + y
    end

    @test d == DataFrame(a = 51, b = 2, x = 1)
end


end # module