module TestAsTableFlag

using Test
using DataFramesMeta
using Statistics

const ≅ = isequal

@testset "@astable with just assignments" begin
    df = DataFrame(a = 1, b = 2)

    d = @rtransform df @astable begin
        :x = 1

        nothing
    end

    @test d == DataFrame(a = 1, b = 2, x = 1)

    d = @rselect df @astable begin
        :x = 1
        y = 100
        nothing
    end

    @test d == DataFrame(x = 1)

    d = @transform df @astable begin
        :x = [5]
        y = 100
        nothing
    end

    @test d == DataFrame(a = 1, b = 2, x = 5)

    d = @select df @astable begin
        :x = [5]
        y = 100
        nothing
    end

    @test d == DataFrame(x = 5)
end

@testset "@astable with just assignments, mutating" begin
    # After finalizing above testset
end

@testset "@astable with strings" begin
    df = DataFrame(a = 1, b = 2)

    x_str = "x"
    d = @rtransform df @astable begin
        $x_str = 1
        y = 100
        nothing
    end

    @test d == DataFrame(a = 1, b = 2, x = 1)

    d = @rselect df @astable begin
        $x_str = 1
        y = 100
        nothing
    end

    @test d == DataFrame(x = 1)

    d = @transform df @astable begin
        $x_str = [5]
        y = 100
        nothing
    end

    @test d == DataFrame(a = 1, b = 2, x = 5)

    d = @select df @astable begin
        $x_str = [5]
        y = 100
        nothing
    end

    @test d == DataFrame(x = 5)
end

@testset "Re-using variables" begin
    df = DataFrame(a = 1, b = 2)

    d = @rtransform df @astable begin
        :x = 1
        y = 5
        :z = :x + y
    end

    @test d == DataFrame(a = 1, b = 2, x = 1, z = 6)

    d = @rselect df @astable begin
        :x = 1
        y = 5
        :z = :x + y
    end

    @test d == DataFrame(x = 1, z = 6)

    x_str = "x"
    d = @rtransform df @astable begin
        $x_str = 1
        y = 5
        :z = $x_str + y
    end

    @test d == DataFrame(a = 1, b = 2, x = 1, z = 6)

    d = @rselect df @astable begin
        $x_str = 1
        y = 5
        :z = $x_str + y
    end

    @test d == DataFrame(x = 1, z = 6)
end

@testset "grouping astable flag" begin
    df = DataFrame(a = [1, 1, 2, 2], b = [5, 6, 7, 8])

    gd = groupby(df, :a)

    d = @combine gd @astable begin
        ex = extrema(:b)
        :b_min = ex[1]
        :b_max = ex[2]
    end

    res_sorted = DataFrame(a = [1, 2], b_min = [5, 7], b_max = [6, 8])

    @test sort(d, :b_min) == res_sorted

    d = @combine gd @astable begin
        ex = extrema(:b)
        $"b_min" = ex[1]
        $"b_max" = ex[2]
    end

    @test sort(d, :b_min) == res_sorted

    d = @by df :a @astable begin
        ex = extrema(:b)
        :b_min = ex[1]
        :b_max = ex[2]
    end

    @test sort(d, :b_min) == res_sorted

    d = @by df :a @astable begin
        ex = extrema(:b)
        $"b_min" = ex[1]
        $"b_max" = ex[2]
    end

    @test sort(d, :b_min) == res_sorted
end

@testset "errors with passmissing" begin
    @eval df = DataFrame(y = 1)
    @test_throws LoadError @eval @transform df @passmising @byrow @astable :x = 2
    @test_throws LoadError @eval @transform df @byrow @astable @passmissing :x = 2
    @test_throws LoadError @eval @transform df @astable @passmissing @byrow :x = 2
    @test_throws LoadError @eval @rtransform df @astable @passmissing :x = 2
    @test_throws LoadError @eval @rtransform df @passmissing @astable :x = 2
end

@testset "bad assignments" begin
    @eval df = DataFrame(y = 1)
    @test_throws ArgumentError @eval @transform df @astable cols(1) = :y
    @test_throws ArgumentError @eval @transform df @astable cols(AsTable) = :y
end

end # module