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


end # module