module Testeachrow

using Test
using DataFrames
using DataFramesMeta
using Statistics

const ≅ = isequal

# `y` needs to be in global scope here because the testset relies on `y`
y = 0
@testset "eachrow" begin
    df = DataFrame(A = 1:3, B = [2, 1, 2])

    @test @eachrow(df, if :A > :B; :A = 0 end) == DataFrame(A = [1, 0, 0], B = [2, 1, 2])

    @test  df == DataFrame(A = [1, 2, 3], B = [2, 1, 2])

    df = DataFrame(A = 1:3, B = [2, 1, 2])  # Restore df
    @eachrow(df, if :A + :B == 3; global y += 1 end)
    @test  y == 2

    df = DataFrame(A = 1:3, B = [2, 1, 2])
    df2 = @eachrow df begin
        @newcol colX::Array{Float64}
        @newcol colY::Array{Float64}
        :colX = :B == 2 ? pi * :A : :B
        if :A > 1
            :colY = :A * :B
        end
    end

    @test  df2.colX == [pi, 1.0, 3pi]
    @test  df2[2, :colY] == 2

    df = DataFrame(b = [5], a = [(n1 = 1, n2 = 2)])
    df2 = @eachrow df begin
        :b = :a.n1
    end

    @test df2.b == [1]

    _DF = 5
    _N = 0
    @eachrow df begin
        # nothing
    end
    @test _DF == 5
    @test _N == 0

    df3 = @eachrow df begin
        :b = row
    end
    @test df3.b == [1]

    x = (a = 400, b = 600)
    df4 = @eachrow df begin
        :b = x.a
    end
    @test df4.b == [400]
end

@testset "cols with @eachrow" begin
    df = DataFrame(A = 1:3, B = [2, 1, 2])
    n = :A
    df2 = @eachrow df begin
        :B = cols(n)
    end
    @test df2 == DataFrame(A = 1:3, B = 1:3)

    n = "A"
    df2 = @eachrow df begin
        :B = cols(n)
    end
    @test df2 == DataFrame(A = 1:3, B = 1:3)

    df2 = @eachrow df begin
        :A = cols(:A) + cols("B")
    end
    @test df2.A == df.A + df.B

    n = :A
    df2 = @eachrow df begin
        cols(n) = :B
    end
    @test df2 == DataFrame(A = [2, 1, 2], B = [2, 1, 2])

    n = "A"
    df2 = @eachrow df begin
        cols(n) = :B
    end
    @test df2 == DataFrame(A = [2, 1, 2], B = [2, 1, 2])

    n = :C
    df2 = @eachrow df begin
        @newcol cols(n)::Vector{Int}
        cols(n) = :B
    end
    @test df2 == DataFrame(A = [1, 2, 3], B = [2, 1, 2], C = [2, 1, 2])

    n = "C"
    df2 = @eachrow df begin
        @newcol cols(n)::Vector{Int}
        cols(n) = :B
    end
    @test df2 == DataFrame(A = [1, 2, 3], B = [2, 1, 2], C = [2, 1, 2])
end

df = DataFrame(A = 1:3, B = [2, 1, 2])

@testset "limits of @eachrow" begin
    @eval Testeachrow n = ["A", "B"]
    @test_throws ArgumentError @eval @eachrow df begin cols(n) end

    @eval Testeachrow n = [:A, :B]
    @test_throws ArgumentError @eval @eachrow df begin cols(n) end

    @eval Testeachrow n = [1, 2]
    @test_throws ArgumentError @eval @eachrow df begin cols(n) end

    @test_throws ArgumentError @eachrow df cols(1) + cols(:A)
end

end # module