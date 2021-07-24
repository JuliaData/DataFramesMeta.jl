module Testbyrow

using Test
using DataFrames
using DataFramesMeta
using Statistics

const ≅ = isequal

@testset "@transform with $AsTable" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing])

    d = @transform df $AsTable =  (a = 1, b = 2)

    @test d ≅ transform(df, [] => (() -> (a = 1, b = 2)) => AsTable)

    d = @transform df $AsTable =  (a = :i, b = :g)

    @test d.a == df.i

    d = @transform df begin
        a = 1
        $AsTable =  (a1 = :i, a2 = :g)
    end

    @test d.a1 == df.i
end

@testset "@select with $AsTable = " begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing])

    d = @select df $AsTable =  (a = 1, b = 2)

    @test d ≅ select(df, [] => (() -> (a = 1, b = 2)) => AsTable)

    d = @select df $AsTable =  (a = :i, b = :g)

    @test d.a == df.i

    d = @select df begin
        a = 1
        $AsTable =  (a1 = :i, a2 = :g)
    end

    @test d.a1 == df.i
end

@testset "@transform! with $AsTable = " begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing])

    d = @transform! copy(df) $AsTable =  (a = 1, b = 2)

    @test d ≅ transform(df, [] => (() -> (a = 1, b = 2)) => AsTable)

    d = @transform! copy(df) $AsTable =  (a = :i, b = :g)

    @test d.a == df.i

    d = @transform! copy(df) begin
        a = 1
        $AsTable =  (a1 = :i, a2 = :g)
    end

    @test d.a1 == df.i
end

@testset "@select! with $AsTable = " begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing])

    d = @select! copy(df) $AsTable =  (a = 1, b = 2)

    @test d ≅ select(df, [] => (() -> (a = 1, b = 2)) => AsTable)

    d = @select! copy(df) $AsTable =  (a = :i, b = :g)

    @test d.a == df.i

    d = @select! copy(df) begin
        a = 1
        $AsTable =  (a1 = :i, a2 = :g)
    end

    @test d.a1 == df.i
end

@testset "@combine with $AsTable = " begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing])

    g = groupby(df, :g)

    d = @combine g $AsTable =  (a = 1, b = 2)

    @test d ≅ combine(g, [] => (() -> (a = 1, b = 2)) => AsTable)

    d = @combine df $AsTable =  (a = first(:i), b = first(:g))

    @test d == DataFrame(a = 1, b = 1)

    d = @combine df begin
        a = 1
        $AsTable =  (a1 = 1, a2 = 2)
    end

    @test d == DataFrame(a = 1, a1 = 1, a2 = 2)
end

@testset "@by with $AsTable = " begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing])

    g = groupby(df, :g)

    d = @by df :g $AsTable =  (a = 1, b = 2)

    @test d ≅ combine(groupby(df, :g), [] => (() -> (a = 1, b = 2)) => AsTable)
end

end