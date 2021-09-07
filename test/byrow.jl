module Testbyrow

using Test
using DataFrames
using DataFramesMeta
using Statistics

const ≅ = isequal

@testset "@transform with @byrow" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing])

    @test @transform(df, @byrow :n = :i + :g) ≅ @transform(df, :n = :i + :g)
    @test @transform(df, @byrow :n = :t * string(:y)) ≅ @transform(df, :n = :t .* string.(:y))
    @test @transform(df, @byrow :n = :g == 1 ? 100 : 500) ≅ @transform(df, :n = ifelse.(:g .== 1, 100, 500))
    @test @transform(df, @byrow :n = :g == 1 && :t == "a") ≅ @transform(df, :n = map((g, t) -> g == 1 && t == "a", :g, :t))
    @test @transform(df, @byrow :n = first(:g)) ≅ @transform(df, :n = first.(:g))

    d = @transform df @byrow begin
        :n1 = :i
        :n2 = :i * :g
    end
    @test d ≅ @transform(df, :n1 = :i, :n2 = :i .* :g)
    @test d ≅ @transform(df, @byrow(:n1 = :i), @byrow(:n2 = :i * :g))

    d = @transform df @byrow begin
        $:n1 = :i
        :n2 = $"i" * :g
    end
    @test d ≅ @transform(df, :n1 = :i, :n2 = :i .* :g)
    d = @transform df @byrow begin
        :n1 = $"i"
        $:n2 = :i * :g
    end
    @test d ≅ @transform(df, :n1 = :i, :n2 = :i .* :g)

    d = @transform df @byrow begin
        :n1 = begin
            :i
        end
        :n2 = :i * :g
    end
    @test d ≅ @transform(df, :n1 = :i, :n2 = :i .* :g)

    d = @transform df @byrow begin
        :n1 = :i * :g
        :n2 = :i * :g
    end
    @test d ≅ @transform(df, :n1 = :i .* :g, :n2 = :i .* :g)
end

@testset "@rtransform" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing])

    @test @rtransform(df, :n = :i + :g) ≅ @transform(df, :n = :i + :g)
    @test @rtransform(df, :n = :t * string(:y)) ≅ @transform(df, :n = :t .* string.(:y))
    @test @rtransform(df, :n = :g == 1 ? 100 : 500) ≅ @transform(df, :n = ifelse.(:g .== 1, 100, 500))
    @test @rtransform(df, :n = :g == 1 && :t == "a") ≅ @transform(df, :n = map((g, t) -> g == 1 && t == "a", :g, :t))
    @test @rtransform(df, :n = first(:g)) ≅ @transform(df, :n = first.(:g))

    d = @rtransform df begin
        :n1 = :i
        :n2 = :i * :g
    end
    @test d ≅ @transform(df, :n1 = :i, :n2 = :i .* :g)
    @test d ≅ @transform(df, @byrow(:n1 = :i), @byrow(:n2 = :i * :g))

    d = @rtransform df begin
        cols(:n1) = :i
        :n2 = cols(:i) * :g
    end
    @test d ≅ @transform(df, :n1 = :i, :n2 = :i .* :g)

    d = @rtransform df begin
        :n1 = cols(:i)
        cols(:n2) = :i * :g
    end
    @test d ≅ @transform(df, :n1 = :i, :n2 = :i .* :g)

    d = @rtransform df begin
        :n1 = begin
            :i
        end
        :n2 = :i * :g
    end
    @test d ≅ @transform(df, :n1 = :i, :n2 = :i .* :g)

    d = @rtransform df begin
        :n1 = :i * :g
        :n2 = :i * :g
    end
    @test d ≅ @transform(df, :n1 = :i .* :g, :n2 = :i .* :g)
end

@testset "@transform! with @byrow" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform!, missing])

    @test @transform!(copy(df), @byrow :n = :i + :g) ≅ @transform!(copy(df), :n = :i + :g)
    @test @transform!(copy(df), @byrow :n = :t * string(:y)) ≅ @transform!(copy(df), :n = :t .* string.(:y))
    @test @transform!(copy(df), @byrow :n = :g == 1 ? 100 : 500) ≅ @transform!(copy(df), :n = ifelse.(:g .== 1, 100, 500))
    @test @transform!(copy(df), @byrow :n = :g == 1 && :t == "a") ≅ @transform!(copy(df), :n = map((g, t) -> g == 1 && t == "a", :g, :t))
    @test @transform!(copy(df), @byrow :n = first(:g)) ≅ @transform!(copy(df), :n = first.(:g))

    d = @transform! df @byrow begin
        :n1 = :i
        :n2 = :i * :g
    end
    @test d ≅ @transform!(copy(df), :n1 = :i, :n2 = :i .* :g)
    @test d ≅ @transform!(copy(df), @byrow(:n1 = :i), @byrow(:n2 = :i * :g))

    d = @transform! df @byrow begin
        $:n1 = :i
        :n2 = $"i" * :g
    end
    @test d ≅ @transform!(copy(df), :n1 = :i, :n2 = :i .* :g)
    d = @transform! df @byrow begin
        :n1 = $"i"
        $:n2 = :i * :g
    end
    @test d ≅ @transform!(copy(df), :n1 = :i, :n2 = :i .* :g)

    d = @transform! df @byrow begin
        :n1 = begin
            :i
        end
        :n2 = :i * :g
    end
    @test d ≅ @transform!(copy(df), :n1 = :i, :n2 = :i .* :g)

    d = @transform! df @byrow begin
        :n1 = :i * :g
        :n2 = :i * :g
    end
    @test d ≅ @transform!(copy(df), :n1 = :i .* :g, :n2 = :i .* :g)
end

@testset "@rtransform!" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform!, missing])

    @test @rtransform!(copy(df), :n = :i + :g) ≅ @transform!(copy(df), :n = :i + :g)
    @test @rtransform!(copy(df), :n = :t * string(:y)) ≅ @transform!(copy(df), :n = :t .* string.(:y))
    @test @rtransform!(copy(df), :n = :g == 1 ? 100 : 500) ≅ @transform!(copy(df), :n = ifelse.(:g .== 1, 100, 500))
    @test @rtransform!(copy(df), :n = :g == 1 && :t == "a") ≅ @transform!(copy(df), :n = map((g, t) -> g == 1 && t == "a", :g, :t))
    @test @rtransform!(copy(df), :n = first(:g)) ≅ @transform!(copy(df), :n = first.(:g))

    df2 = copy(df)
    d = @rtransform! df2 begin
        :n1 = :i
        :n2 = :i * :g
    end
    @test d === df2
    @test d ≅ @transform!(copy(df), :n1 = :i, :n2 = :i .* :g)
    @test d ≅ @transform!(copy(df), @byrow(:n1 = :i), @byrow(:n2 = :i * :g))

    df2 = copy(df)
    d = @rtransform! df2 begin
        cols(:n1) = :i
        :n2 = cols(:i) * :g
    end
    @test d === df2
    @test d ≅ @transform!(copy(df), :n1 = :i, :n2 = :i .* :g)
    d = @rtransform! df begin
        :n1 = cols(:i)
        cols(:n2) = :i * :g
    end
    @test d ≅ @transform!(copy(df), :n1 = :i, :n2 = :i .* :g)

    d = @rtransform! df begin
        :n1 = begin
            :i
        end
        :n2 = :i * :g
    end
    @test d ≅ @transform!(copy(df), :n1 = :i, :n2 = :i .* :g)

    d = @rtransform! df begin
        :n1 = :i * :g
        :n2 = :i * :g
    end
    @test d ≅ @transform!(copy(df), :n1 = :i .* :g, :n2 = :i .* :g)
end


@testset "@select with @byrow" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :select, missing])

    @test @select(df, @byrow :n = :i + :g) ≅ @select(df, :n = :i + :g)
    @test @select(df, @byrow :n = :t * string(:y)) ≅ @select(df, :n = :t .* string.(:y))
    @test @select(df, @byrow :n = :g == 1 ? 100 : 500) ≅ @select(df, :n = ifelse.(:g .== 1, 100, 500))
    @test @select(df, @byrow :n = :g == 1 && :t == "a") ≅ @select(df, :n = map((g, t) -> g == 1 && t == "a", :g, :t))
    @test @select(df, @byrow :n = first(:g)) ≅ @select(df, :n = first.(:g))

    d = @select df @byrow begin
        :n1 = :i
        :n2 = :i * :g
    end
    @test d ≅ @select(df, :n1 = :i, :n2 = :i .* :g)
    @test d ≅ @select(df, @byrow(:n1 = :i), @byrow(:n2 = :i * :g))

    d = @select df @byrow begin
        $:n1 = :i
        :n2 = $"i" * :g
    end
    @test d ≅ @select(df, :n1 = :i, :n2 = :i .* :g)
    d = @select df @byrow begin
        :n1 = $"i"
        $:n2 = :i * :g
    end
    @test d ≅ @select(df, :n1 = :i, :n2 = :i .* :g)

    d = @select df @byrow begin
        :n1 = begin
            :i
        end
        :n2 = :i * :g
    end
    @test d ≅ @select(df, :n1 = :i, :n2 = :i .* :g)

    d = @select df @byrow begin
        :n1 = :i * :g
        :n2 = :i * :g
    end
    @test d ≅ @select(df, :n1 = :i .* :g, :n2 = :i .* :g)
end

@testset "@rselect" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :select, missing])

    @test @rselect(df, :n = :i + :g) ≅ @select(df, :n = :i + :g)
    @test @rselect(df, :n = :t * string(:y)) ≅ @select(df, :n = :t .* string.(:y))
    @test @rselect(df, :n = :g == 1 ? 100 : 500) ≅ @select(df, :n = ifelse.(:g .== 1, 100, 500))
    @test @rselect(df, :n = :g == 1 && :t == "a") ≅ @select(df, :n = map((g, t) -> g == 1 && t == "a", :g, :t))
    @test @rselect(df, :n = first(:g)) ≅ @select(df, :n = first.(:g))

    d = @rselect df begin
        :n1 = :i
        :n2 = :i * :g
    end
    @test d ≅ @select(df, :n1 = :i, :n2 = :i .* :g)
    @test d ≅ @select(df, @byrow(:n1 = :i), @byrow(:n2 = :i * :g))

    d = @rselect df begin
        cols(:n1) = :i
        :n2 = cols(:i) * :g
    end
    @test d ≅ @select(df, :n1 = :i, :n2 = :i .* :g)
    d = @rselect df begin
        :n1 = cols(:i)
        cols(:n2) = :i * :g
    end
    @test d ≅ @select(df, :n1 = :i, :n2 = :i .* :g)

    d = @rselect df begin
        :n1 = begin
            :i
        end
        :n2 = :i * :g
    end
    @test d ≅ @select(df, :n1 = :i, :n2 = :i .* :g)

    d = @rselect df begin
        :n1 = :i * :g
        :n2 = :i * :g
    end
    @test d ≅ @select(df, :n1 = :i .* :g, :n2 = :i .* :g)
end

@testset "@select! with @byrow" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :select!, missing])

    @test @select!(copy(df), @byrow :n = :i + :g) ≅ @select!(copy(df), :n = :i + :g)
    @test @select!(copy(df), @byrow :n = :t * string(:y)) ≅ @select!(copy(df), :n = :t .* string.(:y))
    @test @select!(copy(df), @byrow :n = :g == 1 ? 100 : 500) ≅ @select!(copy(df), :n = ifelse.(:g .== 1, 100, 500))
    @test @select!(copy(df), @byrow :n = :g == 1 && :t == "a") ≅ @select!(copy(df), :n = map((g, t) -> g == 1 && t == "a", :g, :t))
    @test @select!(copy(df), @byrow :n = first(:g)) ≅ @select!(copy(df), :n = first.(:g))

    d = @select! copy(df) @byrow begin
        :n1 = :i
        :n2 = :i * :g
    end
    @test d ≅ @select!(copy(df), :n1 = :i, :n2 = :i .* :g)
    @test d ≅ @select!(copy(df), @byrow(:n1 = :i), @byrow(:n2 = :i * :g))

    d = @select! copy(df) @byrow begin
        $:n1 = :i
        :n2 = $"i" * :g
    end
    @test d ≅ @select!(copy(df), :n1 = :i, :n2 = :i .* :g)
    d = @select! copy(df) @byrow begin
        :n1 = $"i"
        $:n2 = :i * :g
    end
    @test d ≅ @select!(copy(df), :n1 = :i, :n2 = :i .* :g)

    d = @select! copy(df) @byrow begin
        :n1 = begin
            :i
        end
        :n2 = :i * :g
    end
    @test d ≅ @select!(copy(df), :n1 = :i, :n2 = :i .* :g)

    d = @select! copy(df) @byrow begin
        :n1 = :i * :g
        :n2 = :i * :g
    end
    @test d ≅ @select!(copy(df), :n1 = :i .* :g, :n2 = :i .* :g)
end

@testset "@rselect!" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :select!, missing])

    @test @rselect!(copy(df), :n = :i + :g) ≅ @select!(copy(df), :n = :i + :g)
    @test @rselect!(copy(df), :n = :t * string(:y)) ≅ @select!(copy(df), :n = :t .* string.(:y))
    @test @rselect!(copy(df), :n = :g == 1 ? 100 : 500) ≅ @select!(copy(df), :n = ifelse.(:g .== 1, 100, 500))
    @test @rselect!(copy(df), :n = :g == 1 && :t == "a") ≅ @select!(copy(df), :n = map((g, t) -> g == 1 && t == "a", :g, :t))
    @test @rselect!(copy(df), :n = first(:g)) ≅ @select!(copy(df), :n = first.(:g))

    df2 = copy(df)
    d = @rselect! df2 begin
        :n1 = :i
        :n2 = :i * :g
    end
    @test d === df2
    @test d ≅ @select!(copy(df), :n1 = :i, :n2 = :i .* :g)
    @test d ≅ @select!(copy(df), @byrow(:n1 = :i), @byrow(:n2 = :i * :g))

    df2 = copy(df)
    d = @rselect! df2 begin
        cols(:n1) = :i
        :n2 = cols(:i) * :g
    end
    @test d === df2
    @test d ≅ @select!(copy(df), :n1 = :i, :n2 = :i .* :g)
    d = @rselect! copy(df) begin
        :n1 = cols(:i)
        cols(:n2) = :i * :g
    end
    @test d ≅ @select!(copy(df), :n1 = :i, :n2 = :i .* :g)

    d = @rselect! copy(df) begin
        :n1 = begin
            :i
        end
        :n2 = :i * :g
    end
    @test d ≅ @select!(copy(df), :n1 = :i, :n2 = :i .* :g)

    d = @rselect! copy(df) begin
        :n1 = :i * :g
        :n2 = :i * :g
    end
    @test d ≅ @select!(copy(df), :n1 = :i .* :g, :n2 = :i .* :g)
end

@testset "@with with @byrow" begin
    df = DataFrame(A = 1:3, B = [2, 1, 2])

    @test @with(df, @byrow :A * 1)   ==  df.A .* 1
    @test @with(df, @byrow :A * :B)  ==  df.A .* df.B

    t = @with df @byrow begin
        :A * 1
    end
    @test t == df.A .* 1

    t = @with df @byrow begin
        :A * :B
    end
    @test t == df.A .* df.B
end

@testset "@subset with @byrow" begin
    df = DataFrame(A = [1, 2, 3, missing], B = [2, 1, 2, 1])

    d = @subset df begin
        @byrow :A > 1
        @byrow :B > 1
    end
    @test d ≅ @subset(df, :A .> 1, :B .> 1)

    d = @subset df @byrow begin
        :A > 1
        :B > 1
    end
    @test d ≅ @subset(df, :A .> 1, :B .> 1)
end

@testset "@rsubset" begin
    df = DataFrame(A = [1, 2, 3, missing], B = [2, 1, 2, 1])

    d = @rsubset df begin
        :A > 1
        :B > 1
    end
    @test d ≅ @subset(df, :A .> 1, :B .> 1)

    d = @rsubset df begin
        :A > 1
        :B > 1
    end
    @test d ≅ @subset(df, :A .> 1, :B .> 1)
end

@testset "@subset! with @byrow" begin
    df = DataFrame(A = [1, 2, 3, missing], B = [2, 1, 2, 1])

    d = @subset! copy(df) begin
        @byrow :A > 1
        @byrow :B > 1
    end
    @test d ≅ @subset(df, :A .> 1, :B .> 1)

    d = @subset! copy(df) @byrow begin
        :A > 1
        :B > 1
    end
    @test d ≅ @subset(df, :A .> 1, :B .> 1)
end

@testset "@rsubset!" begin
    df = DataFrame(A = [1, 2, 3, missing], B = [2, 1, 2, 1])

    d = @rsubset! copy(df) begin
        :A > 1
        :B > 1
    end
    @test d ≅ @subset(df, :A .> 1, :B .> 1)

    d = @rsubset! copy(df) begin
        :A > 1
        :B > 1
    end
    @test d ≅ @subset(df, :A .> 1, :B .> 1)
end

@testset "@orderby with @byrow" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing]
        )

    d = @orderby df begin
        @byrow :c
        @byrow :g *  2
    end
    @test d ≅ @orderby(df, :c, :g .* 2)

    d = @orderby df @byrow begin
        :c
        :g *  2
    end
    @test d ≅ @orderby(df, :c, :g .* 2)
end

@testset "@rorderby " begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing]
        )

    d = @rorderby df begin
        :c
        :g *  2
    end
    @test d ≅ @orderby(df, :c, :g .* 2)

    d = @rorderby df begin
        :c
        :g *  2
    end
    @test d ≅ @orderby(df, :c, :g .* 2)
end

@testset "@combine with @byrow" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :combine, missing])

    gd = groupby(df, :g)

    @test @combine(gd, @byrow :n = :i + :g) ≅ @combine(gd, :n = :i + :g)
    @test @combine(gd, @byrow :n = :t * string(:y)) ≅ @combine(gd, :n = :t .* string.(:y))
    @test @combine(gd, @byrow :n = :g == 1 ? 100 : 500) ≅ @combine(gd, :n = ifelse.(:g .== 1, 100, 500))
    @test @combine(gd, @byrow :n = :g == 1 && :t == "a") ≅ @combine(gd, :n = map((g, t) -> g == 1 && t == "a", :g, :t))
    @test @combine(gd, @byrow :n = first(:g)) ≅ @combine(gd, :n = first.(:g))

    d = @combine gd @byrow begin
        :n1 = :i
        :n2 = :i * :g
    end
    @test d ≅ @combine(gd, :n1 = :i, :n2 = :i .* :g)
    @test d ≅ @combine(gd, @byrow(:n1 = :i), @byrow(:n2 = :i * :g))

    d = @combine gd @byrow begin
        $:n1 = :i
        :n2 = $"i" * :g
    end
    @test d ≅ @combine(gd, :n1 = :i, :n2 = :i .* :g)
    d = @combine gd @byrow begin
        :n1 = $"i"
        $:n2 = :i * :g
    end
    @test d ≅ @combine(gd, :n1 = :i, :n2 = :i .* :g)

    d = @combine gd @byrow begin
        :n1 = begin
            :i
        end
        :n2 = :i * :g
    end
    @test d ≅ @combine(gd, :n1 = :i, :n2 = :i .* :g)

    d = @combine gd @byrow begin
        :n1 = :i * :g
        :n2 = :i * :g
    end
    @test d ≅ @combine(gd, :n1 = :i .* :g, :n2 = :i .* :g)
end

@testset "@by with @byrow" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :combine, missing])

    @test @by(df, :g, @byrow :n = :i + :g) ≅ @by(df, :g, :n = :i + :g)
    @test @by(df, :g, @byrow :n = :t * string(:y)) ≅ @by(df, :g, :n = :t .* string.(:y))
    @test @by(df, :g, @byrow :n = :g == 1 ? 100 : 500) ≅ @by(df, :g, :n = ifelse.(:g .== 1, 100, 500))
    @test @by(df, :g, @byrow :n = :g == 1 && :t == "a") ≅ @by(df, :g, :n = map((g, t) -> g == 1 && t == "a", :g, :t))
    @test @by(df, :g, @byrow :n = first(:g)) ≅ @by(df, :g, :n = first.(:g))

    d = @by df :g @byrow begin
        :n1 = :i
        :n2 = :i * :g
    end
    @test d ≅ @by(df, :g, :n1 = :i, :n2 = :i .* :g)
    @test d ≅ @by(df, :g, @byrow(:n1 = :i), @byrow(:n2 = :i * :g))

    d = @by df :g @byrow begin
        $:n1 = :i
        :n2 = $"i" * :g
    end
    @test d ≅ @by(df, :g, :n1 = :i, :n2 = :i .* :g)

    d = @by df :g @byrow begin
        :n1 = $"i"
        $:n2 = :i * :g
    end
    @test d ≅ @by(df, :g, :n1 = :i, :n2 = :i .* :g)

    d = @by df :g @byrow begin
        :n1 = begin
            :i
        end
        :n2 = :i * :g
    end
    @test d ≅ @by(df, :g, :n1 = :i, :n2 = :i .* :g)

    d = @by df :g @byrow begin
        :n1 = :i * :g
        :n2 = :i * :g
    end
    @test d ≅ @by(df, :g, :n1 = :i .* :g, :n2 = :i .* :g)
end

@testset "multi-argument r*, #280" begin
    df = DataFrame(a = [1, 2, 3], b = [4, 5, 6])
    d = @rtransform(df, :x = :a, :y = :a + :b)
    @test d.x == df.a

    d = @rtransform!(copy(df), :x = :a, :y = :a + :b)
    @test d.x == df.a

    d = @rselect(df, :x = :a, :y = :a + :b)
    @test d.x == df.a

    d = @rselect!(copy(df), :x = :a, :y = :a + :b)
    @test d.x == df.a

    d = @rsubset(df, :a > 1, :b > 3)
    @test d == DataFrame(a = [2, 3], b = [5, 6])

    d = @rsubset!(copy(df), :a > 1, :b > 3)
    @test d == DataFrame(a = [2, 3], b = [5, 6])

    d = @rorderby(df, -:a, -:b)
    @test d == DataFrame(a = [3, 2, 1], b = [6, 5, 4])

    # no `@orderby!` for some reason.
end

end