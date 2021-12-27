module TestFunctionCompilation

using Test
using DataFramesMeta

@testset "function_compilation" begin
    @eval begin
        df = DataFrame(a = [1], b = [2])

        testfun(x, y) = x .* y
        testdotfun(x, y) = x * y
        testnt(x) = (c = x,)
    end

    # Lazy way of making sure all functions are pre-compiled.
    # @eval prevents julia from caching the intermediate anonymous functions.
    for _ in 1:2
        @eval begin
            @test @select(df, :c =  :a + :b) == DataFrame(c = [3])

            fasttime = @timed @select(df, :c =  :a + :b)
            slowtime = @timed select(df, [:a, :b] => ((a, b) -> a + b) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, :c =  begin :a + :b end) == DataFrame(c = [3])

            fasttime = @timed @select(df, :c =  begin :a + :b end)
            slowtime = @timed select(df, [:a, :b] => ((a, b) -> a + b) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, cols(:c) = :a + :b) == DataFrame(c = [3])

            fasttime = @timed @select(df, cols(:c) = :a + :b)
            slowtime = @timed select(df, [:a, :b] => ((a, b) -> a + b) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, :c =  :a .+ :b) == DataFrame(c = [3])

            fasttime = @timed @select(df, :c =  :a .+ :b)
            slowtime = @timed select(df, [:a, :b] => ((a, b) -> a .+ b) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, cols(:c) = :a .+ :b) == DataFrame(c = [3])

            fasttime = @timed @select(df, cols(:c) = :a .+ :b)
            slowtime = @timed select(df, [:a, :b] => ((a, b) -> a .+ b) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, :c =  cols(:a) + cols(:b)) == DataFrame(c = [3])

            fasttime = @timed @select(df, :c =  cols(:a) + cols(:b))
            slowtime = @timed select(df, [:a, :b] => ((a, b) -> a + b) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, cols(:c) = cols(:a) + cols(:b)) == DataFrame(c = [3])

            fasttime = @timed @select(df, cols(:c) = cols(:a) + cols(:b))
            slowtime = @timed select(df, [:a, :b] => ((a, b) -> a + b) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, :c =  cols(:a) .+ cols(:b)) == DataFrame(c = [3])

            fasttime = @timed @select(df, :c =  cols(:a) .+ cols(:b))
            slowtime = @timed select(df, [:a, :b] => ((a, b) -> a .+ b) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, cols(:c) = cols(:a) .+ cols(:b)) == DataFrame(c = [3])

            fasttime = @timed @select(df, cols(:c) = cols(:a) .+ cols(:b))
            slowtime = @timed select(df, [:a, :b] => ((a, b) -> a .+ b) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, :c =  :a) == DataFrame(c = [1])

            fasttime = @timed @select(df, :c =  :a)
            slowtime = @timed select(df, [:a] => (a -> identity(a)) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, cols(:c) = :a) == DataFrame(c = [1])

            fasttime = @timed @select(df, cols(:c) = :a)
            slowtime = @timed select(df, [:a] => (a -> identity(a)) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, :c =  cols(:a)) == DataFrame(c = [1])

            fasttime = @timed @select(df, :c =  cols(:a))
            slowtime = @timed select(df, [:a] => (a -> identity(a)) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, cols(:c) = cols(:a)) == DataFrame(c = [1])

            fasttime = @timed @select(df, cols(:c) = cols(:a))
            slowtime = @timed select(df, [:a] => (a -> identity(a)) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, :a) == df[:, [:a]]

            fasttime = @timed @select(df, :a)
            slowtime = @timed select(df, [:a] => (a -> identity(a)) => :a)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, cols(:a)) == df[:, [:a]]

            fasttime = @timed @select(df, cols(:a))
            slowtime = @timed select(df, [:a] => (a -> identity(a)) => :a)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, :c =  testfun(:a, :b)) == DataFrame(c = [2])

            fasttime = @timed @select(df, :c =  testfun(:a, :b))
            slowtime = @timed select(df, [:a, :b] => ((a, b) -> testfun(a, b)) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, cols(:c) = testfun(:a, :b)) == DataFrame(c = [2])

            fasttime = @timed @select(df, cols(:c) = testfun(:a, :b))
            slowtime = @timed select(df, [:a, :b] => ((a, b) -> testfun(a, b)) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, :c =  testfun(cols("a"), cols("b"))) == DataFrame(c = [2])

            fasttime = @timed @select(df, :c =  testfun(cols("a"), cols("b")))
            slowtime = @timed select(df, [:a, :b] => ((a, b) -> testfun(a, b)) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, cols(:c) = testfun(cols("a"), cols("b"))) == DataFrame(c = [2])

            fasttime = @timed @select(df, cols(:c) = testfun(cols("a"), cols("b")))
            slowtime = @timed select(df, [:a, :b] => ((a, b) -> testfun(a, b)) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, :c =  testdotfun.(:a, :b)) == DataFrame(c = [2])

            fasttime = @timed @select(df, :c =  testdotfun.(:a, :b))
            slowtime = @timed select(df, [:a, :b] => ((a, b) -> testdotfun.(a, b)) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, cols(:c) = testdotfun.(:a, :b)) == DataFrame(c = [2])

            fasttime = @timed @select(df, cols(:c) = testdotfun.(:a, :b))
            slowtime = @timed select(df, [:a, :b] => ((a, b) -> testdotfun.(a, b)) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, :c =  testdotfun.(cols("a"), cols("b"))) == DataFrame(c = [2])

            fasttime = @timed @select(df, :c =  testdotfun.(cols("a"), cols("b")))
            slowtime = @timed select(df, [:a, :b] => ((a, b) -> testdotfun.(a, b)) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, cols(:c) = testdotfun.(cols("a"), cols("b"))) == DataFrame(c = [2])

            fasttime = @timed @select(df, cols(:c) = testdotfun.(cols("a"), cols("b")))
            slowtime = @timed select(df, [:a, :b] => ((a, b) -> testdotfun.(a, b)) => :c)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            gd = groupby(df, :a)

            @test @combine(gd, cols(AsTable) = testnt(:b)) == DataFrame(a = [1], c =  [2])

            fasttime = @timed @combine(gd, cols(AsTable) = testnt(:b))
            slowtime = @timed combine(gd, :b => (b -> testnt(b)) => AsTable)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @with df (:a + :b) == [3]

            fasttime = @timed @with df (:a + :b)
            slowtime = @timed @with df ((a, b) -> a + b)(df.a, df.b)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @with df (:a .* :b) == [2]

            @with df (:a .* :b)
            fasttime = @timed @with df (:a .* :b)
            slowtime = @timed @with df ((a, b) -> a .* b)(df.a, df.b)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @with df testfun(:a, :b) == [2]

            fasttime = @timed @with df testfun(:a, :b)
            slowtime = @timed @with df ((a, b) -> testfun(a, b))(df.a, df.b)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @with df testdotfun.(:a, :b) == [2]

            fasttime = @timed @with df testdotfun.(:a, :b)
            slowtime = @timed @with df ((a, b) -> testdotfun.(a, b))(df.a, df.b)
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")
        end
    end
end

@testset "composed compilation" begin
    @eval begin
        df = DataFrame(a = [1], b = [2])

        f(x) = identity(x)
        g(x, y) = x + y

        df_wide = DataFrame(rand(10, 1000), :auto)
    end

    for _ in 1:2
        @eval begin
            @test @select(df, :y = (f ∘ g)(:a, :b)).y == [3]

            fasttime = @timed @select(df, :y = (f ∘ g)(:a, :b))
            slowtime = @timed select(df, [:a, :b] => ((a, b) -> (f ∘ g)(a, b)) => :y )
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            @test @select(df, :y = (f ∘ g).(:a, :b)).y == [3]

            fasttime = @timed @select(df, :y = (f ∘ g).(:a, :b))
            slowtime = @timed select(df, [:a, :b] => ((a, b) -> (f ∘ g).(a, b)) => :y )
            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")

            fasttime = @timed @rselect df_wide :y = (sum ∘ skipmissing)(AsTable(:))
            slowtime = @timed select(df_wide, AsTable(:) => ByRow(t -> (sum ∘ skipmissing)(t)) => :y)

            (slowtime[2] > fasttime[2]) || @warn("Slow compilation")
        end
    end

end

end # module