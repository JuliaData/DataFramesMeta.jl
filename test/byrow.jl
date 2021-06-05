@testset "@transform with @byrow" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing])

    @test @transform(df, n = @byrow :i + :g) ≅ @transform(df, n = :i + :g)
    @test @transform(df, n = @byrow :t * string(:y)) ≅ @transform(df, n = :t .* string.(:y))
    @test @transform(df, transform = @byrow :i) ≅ @transform(df, transform = :i)
    @test @transform(df, n = @byrow :g == 1 ? 100 : 500) ≅ @transform(df, n = ifelse.(:g .== 1, 100, 500))
    @test @transform(df, n = @byrow :g == 1 && :t == "a") ≅ @transform(df, n = map((g, t) -> g == 1 && t == "a", :g, :t))
    @test @transform(df, n = @byrow first(:g)) ≅ @transform(df, n = first.(:g))

    d = @transform df @byrow begin
        n1 = :i
        n2 = :i * :g
    end
    @test d ≅ @transform(df, n1 = :i, n2 = :i .* :g)
    @test d ≅ @transform(df, n1 = @byrow(:i), n2 = @byrow(:i * :g))

    d = @transform df @byrow begin
        cols(:n1) = :i
        n2 = cols(:i) * :g
    end
    @test d ≅ @transform(df, n1 = :i, n2 = :i .* :g)
    d = @transform df @byrow begin
        n1 = cols(:i)
        cols(:n2) = :i * :g
    end
    @test d ≅ @transform(df, n1 = :i, n2 = :i .* :g)

    d = @transform df @byrow begin
        n1 = begin
            :i
        end
        n2 = :i * :g
    end
    @test d ≅ @transform(df, n1 = :i, n2 = :i .* :g)

    d = @transform df @byrow begin
        n1 = :i * :g
        n2 = :i * :g
    end
    @test d ≅ @transform(df, n1 = :i .* :g, n2 = :i .* :g)
end

@testset "@transform! with @byrow" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform!, missing])

    @test @transform!(copy(df), n = @byrow :i + :g) ≅ @transform!(copy(df), n = :i + :g)
    @test @transform!(copy(df), n = @byrow :t * string(:y)) ≅ @transform!(copy(df), n = :t .* string.(:y))
    @test @transform!(copy(df), transform! = @byrow :i) ≅ @transform!(copy(df), transform! = :i)
    @test @transform!(copy(df), n = @byrow :g == 1 ? 100 : 500) ≅ @transform!(copy(df), n = ifelse.(:g .== 1, 100, 500))
    @test @transform!(copy(df), n = @byrow :g == 1 && :t == "a") ≅ @transform!(copy(df), n = map((g, t) -> g == 1 && t == "a", :g, :t))
    @test @transform!(copy(df), n = @byrow first(:g)) ≅ @transform!(copy(df), n = first.(:g))

    d = @transform! df @byrow begin
        n1 = :i
        n2 = :i * :g
    end
    @test d ≅ @transform!(copy(df), n1 = :i, n2 = :i .* :g)
    @test d ≅ @transform!(copy(df), n1 = @byrow(:i), n2 = @byrow(:i * :g))

    d = @transform! df @byrow begin
        cols(:n1) = :i
        n2 = cols(:i) * :g
    end
    @test d ≅ @transform!(copy(df), n1 = :i, n2 = :i .* :g)
    d = @transform! df @byrow begin
        n1 = cols(:i)
        cols(:n2) = :i * :g
    end
    @test d ≅ @transform!(copy(df), n1 = :i, n2 = :i .* :g)

    d = @transform! df @byrow begin
        n1 = begin
            :i
        end
        n2 = :i * :g
    end
    @test d ≅ @transform!(copy(df), n1 = :i, n2 = :i .* :g)

    d = @transform! df @byrow begin
        n1 = :i * :g
        n2 = :i * :g
    end
    @test d ≅ @transform!(copy(df), n1 = :i .* :g, n2 = :i .* :g)
end

@testset "@select with @byrow" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :select, missing])

    @test @select(df, n = @byrow :i + :g) ≅ @select(df, n = :i + :g)
    @test @select(df, n = @byrow :t * string(:y)) ≅ @select(df, n = :t .* string.(:y))
    @test @select(df, select = @byrow :i) ≅ @select(df, select = :i)
    @test @select(df, n = @byrow :g == 1 ? 100 : 500) ≅ @select(df, n = ifelse.(:g .== 1, 100, 500))
    @test @select(df, n = @byrow :g == 1 && :t == "a") ≅ @select(df, n = map((g, t) -> g == 1 && t == "a", :g, :t))
    @test @select(df, n = @byrow first(:g)) ≅ @select(df, n = first.(:g))

    d = @select df @byrow begin
        n1 = :i
        n2 = :i * :g
    end
    @test d ≅ @select(df, n1 = :i, n2 = :i .* :g)
    @test d ≅ @select(df, n1 = @byrow(:i), n2 = @byrow(:i * :g))

    d = @select df @byrow begin
        cols(:n1) = :i
        n2 = cols(:i) * :g
    end
    @test d ≅ @select(df, n1 = :i, n2 = :i .* :g)
    d = @select df @byrow begin
        n1 = cols(:i)
        cols(:n2) = :i * :g
    end
    @test d ≅ @select(df, n1 = :i, n2 = :i .* :g)

    d = @select df @byrow begin
        n1 = begin
            :i
        end
        n2 = :i * :g
    end
    @test d ≅ @select(df, n1 = :i, n2 = :i .* :g)

    d = @select df @byrow begin
        n1 = :i * :g
        n2 = :i * :g
    end
    @test d ≅ @select(df, n1 = :i .* :g, n2 = :i .* :g)
end

@testset "@select! with @byrow" begin
    copy(df) = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :select!, missing])

    @test @select!(copy(df), n = @byrow :i + :g) ≅ @select!(copy(df), n = :i + :g)
    @test @select!(copy(df), n = @byrow :t * string(:y)) ≅ @select!(copy(df), n = :t .* string.(:y))
    @test @select!(copy(df), select! = @byrow :i) ≅ @select!(copy(df), select! = :i)
    @test @select!(copy(df), n = @byrow :g == 1 ? 100 : 500) ≅ @select!(copy(df), n = ifelse.(:g .== 1, 100, 500))
    @test @select!(copy(df), n = @byrow :g == 1 && :t == "a") ≅ @select!(copy(df), n = map((g, t) -> g == 1 && t == "a", :g, :t))
    @test @select!(copy(df), n = @byrow first(:g)) ≅ @select!(copy(df), n = first.(:g))

    d = @select! copy(df) @byrow begin
        n1 = :i
        n2 = :i * :g
    end
    @test d ≅ @select!(copy(df), n1 = :i, n2 = :i .* :g)
    @test d ≅ @select!(copy(df), n1 = @byrow(:i), n2 = @byrow(:i * :g))

    d = @select! copy(df) @byrow begin
        cols(:n1) = :i
        n2 = cols(:i) * :g
    end
    @test d ≅ @select!(copy(df), n1 = :i, n2 = :i .* :g)
    d = @select! copy(df) @byrow begin
        n1 = cols(:i)
        cols(:n2) = :i * :g
    end
    @test d ≅ @select!(copy(df), n1 = :i, n2 = :i .* :g)

    d = @select! copy(df) @byrow begin
        n1 = begin
            :i
        end
        n2 = :i * :g
    end
    @test d ≅ @select!(copy(df), n1 = :i, n2 = :i .* :g)

    d = @select! copy(df) @byrow begin
        n1 = :i * :g
        n2 = :i * :g
    end
    @test d ≅ @select!(copy(df), n1 = :i .* :g, n2 = :i .* :g)
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

@testset "where with @byrow" begin
    df = DataFrame(A = [1, 2, 3, missing], B = [2, 1, 2, 1])

    d = @where df begin
        @byrow :A > 1
        @byrow :B > 1
    end
    @test d ≅ @where(df, :A .> 1, :B .> 1)

    d = @where df @byrow begin
        :A > 1
        :B > 1
    end
    @test d ≅ @where(df, :A .> 1, :B .> 1)
end

@testset "orderby with @byrow" begin
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

@testset "@combine with @byrow" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :combine, missing])

    gd = groupby(df, :g)

    @test @combine(gd, n = @byrow :i + :g) ≅ @combine(gd, n = :i + :g)
    @test @combine(gd, n = @byrow :t * string(:y)) ≅ @combine(gd, n = :t .* string.(:y))
    @test @combine(gd, combine = @byrow :i) ≅ @combine(gd, combine = :i)
    @test @combine(gd, n = @byrow :g == 1 ? 100 : 500) ≅ @combine(gd, n = ifelse.(:g .== 1, 100, 500))
    @test @combine(gd, n = @byrow :g == 1 && :t == "a") ≅ @combine(gd, n = map((g, t) -> g == 1 && t == "a", :g, :t))
    @test @combine(gd, n = @byrow first(:g)) ≅ @combine(gd, n = first.(:g))

    d = @combine gd @byrow begin
        n1 = :i
        n2 = :i * :g
    end
    @test d ≅ @combine(gd, n1 = :i, n2 = :i .* :g)
    @test d ≅ @combine(gd, n1 = @byrow(:i), n2 = @byrow(:i * :g))

    d = @combine gd @byrow begin
        cols(:n1) = :i
        n2 = cols(:i) * :g
    end
    @test d ≅ @combine(gd, n1 = :i, n2 = :i .* :g)
    d = @combine gd @byrow begin
        n1 = cols(:i)
        cols(:n2) = :i * :g
    end
    @test d ≅ @combine(gd, n1 = :i, n2 = :i .* :g)

    d = @combine gd @byrow begin
        n1 = begin
            :i
        end
        n2 = :i * :g
    end
    @test d ≅ @combine(gd, n1 = :i, n2 = :i .* :g)

    d = @combine gd @byrow begin
        n1 = :i * :g
        n2 = :i * :g
    end
    @test d ≅ @combine(gd, n1 = :i .* :g, n2 = :i .* :g)
end

@testset "@by with @byrow" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :combine, missing])

    @test @by(df, :g, n = @byrow :i + :g) ≅ @by(df, :g, n = :i + :g)
    @test @by(df, :g, n = @byrow :t * string(:y)) ≅ @by(df, :g, n = :t .* string.(:y))
    @test @by(df, :g, combine = @byrow :i) ≅ @by(df, :g, combine = :i)
    @test @by(df, :g, n = @byrow :g == 1 ? 100 : 500) ≅ @by(df, :g, n = ifelse.(:g .== 1, 100, 500))
    @test @by(df, :g, n = @byrow :g == 1 && :t == "a") ≅ @by(df, :g, n = map((g, t) -> g == 1 && t == "a", :g, :t))
    @test @by(df, :g, n = @byrow first(:g)) ≅ @by(df, :g, n = first.(:g))

    d = @by df :g @byrow begin
        n1 = :i
        n2 = :i * :g
    end
    @test d ≅ @by(df, :g, n1 = :i, n2 = :i .* :g)
    @test d ≅ @by(df, :g, n1 = @byrow(:i), n2 = @byrow(:i * :g))

    d = @by df :g @byrow begin
        cols(:n1) = :i
        n2 = cols(:i) * :g
    end
    @test d ≅ @by(df, :g, n1 = :i, n2 = :i .* :g)

    d = @by df :g @byrow begin
        n1 = cols(:i)
        cols(:n2) = :i * :g
    end
    @test d ≅ @by(df, :g, n1 = :i, n2 = :i .* :g)

    d = @by df :g @byrow begin
        n1 = begin
            :i
        end
        n2 = :i * :g
    end
    @test d ≅ @by(df, :g, n1 = :i, n2 = :i .* :g)

    d = @by df :g @byrow begin
        n1 = :i * :g
        n2 = :i * :g
    end
    @test d ≅ @by(df, :g, n1 = :i .* :g, n2 = :i .* :g)
end