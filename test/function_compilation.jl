module TestFunctionCompilation

using Test
using DataFramesMeta

@testset "function_compilation" begin
	# Lazy way of making sure all
	# functions are pre-compiled.
	for _ in 1:2
		df = DataFrame(a = [1], b = [2])

		testfun(x, y) = x .* y
		testdotfun(x, y) = x * y
		testnt(x) = (c = x,)

		@test @select(df, c = :a + :b) == DataFrame(c = [3])

			fasttime = @timed @select(df, c = :a + :b)
			slowtime = @timed select(df, [:a, :b] => ((a, b) -> a + b) => :c)
			@test slowtime.time > fasttime.time

		@test @select(df, cols(:c) = :a + :b) == DataFrame(c = [3])

			fasttime = @timed @select(df, cols(:c) = :a + :b)
			slowtime = @timed select(df, [:a, :b] => ((a, b) -> a + b) => :c)
			@test slowtime.time > fasttime.time

		@test @select(df, c = :a .+ :b) == DataFrame(c = [3])

			fasttime = @timed @select(df, c = :a .+ :b)
			slowtime = @timed select(df, [:a, :b] => ((a, b) -> a .+ b) => :c)
			@test slowtime.time > fasttime.time

		@test @select(df, cols(:c) = :a .+ :b) == DataFrame(c = [3])

			fasttime = @timed @select(df, cols(:c) = :a .+ :b)
			slowtime = @timed select(df, [:a, :b] => ((a, b) -> a .+ b) => :c)
			@test slowtime.time > fasttime.time

		@test @select(df, c = cols(:a) + cols(:b)) == DataFrame(c = [3])

			fasttime = @timed @select(df, c = cols(:a) + cols(:b))
			slowtime = @timed select(df, [:a, :b] => ((a, b) -> a + b) => :c)
			@test slowtime.time > fasttime.time

		@test @select(df, cols(:c) = cols(:a) + cols(:b)) == DataFrame(c = [3])

			fasttime = @timed @select(df, cols(:c) = cols(:a) + cols(:b))
			slowtime = @timed select(df, [:a, :b] => ((a, b) -> a + b) => :c)
			@test slowtime.time > fasttime.time

		@test @select(df, c = cols(:a) .+ cols(:b)) == DataFrame(c = [3])

			fasttime = @timed @select(df, c = cols(:a) .+ cols(:b))
			slowtime = @timed select(df, [:a, :b] => ((a, b) -> a .+ b) => :c)
			@test slowtime.time > fasttime.time

		@test @select(df, cols(:c) = cols(:a) .+ cols(:b)) == DataFrame(c = [3])

			fasttime = @timed @select(df, cols(:c) = cols(:a) .+ cols(:b))
			slowtime = @timed select(df, [:a, :b] => ((a, b) -> a .+ b) => :c)
			@test slowtime.time > fasttime.time

		@test @select(df, c = :a) == DataFrame(c = [1])

			fasttime = @timed @select(df, c = :a)
			slowtime = @timed select(df, [:a] => (a -> a) => :c)
			@test slowtime.time > fasttime.time

		@test @select(df, cols(:c) = :a) == DataFrame(c = [1])

			fasttime = @timed @select(df, cols(:c) = :a)
			slowtime = @timed select(df, [:a] => (a -> a) => :c)
			@test slowtime.time > fasttime.time

		@test @select(df, c = cols(:a)) == DataFrame(c = [1])

			fasttime = @timed @select(df, c = cols(:a))
			slowtime = @timed select(df, [:a] => (a -> a) => :c)
			@test slowtime.time > fasttime.time

		@test @select(df, cols(:c) = cols(:a)) == DataFrame(c = [1])

			fasttime = @timed @select(df, cols(:c) = cols(:a))
			slowtime = @timed select(df, [:a] => (a -> a) => :c)
			@test slowtime.time > fasttime.time

		@test @select(df, :a) == df[:, [:a]]

			fasttime = @timed @select(df, :a)
			slowtime = @timed select(df, [:a] => (a -> a) => :a)
			@test slowtime.time > fasttime.time

		@test @select(df, cols(:a)) == df[:, [:a]]

			fasttime = @timed @select(df, cols(:a))
			slowtime = @timed select(df, [:a] => (a -> a) => :a)
			@test slowtime.time > fasttime.time

		@test @select(df, c = testfun(:a, :b)) == DataFrame(c = [2])

			fasttime = @timed @select(df, c = testfun(:a, :b))
			slowtime = @timed select(df, [:a, :b] => ((a, b) -> testfun(a, b)) => :c)
			@test slowtime.time > fasttime.time

		@test @select(df, cols(:c) = testfun(:a, :b)) == DataFrame(c = [2])

			fasttime = @timed @select(df, cols(:c) = testfun(:a, :b))
			slowtime = @timed select(df, [:a, :b] => ((a, b) -> testfun(a, b)) => :c)
			@test slowtime.time > fasttime.time

		@test @select(df, c = testfun(cols("a"), cols("b"))) == DataFrame(c = [2])

			fasttime = @timed @select(df, c = testfun(cols("a"), cols("b")))
			slowtime = @timed select(df, [:a, :b] => ((a, b) -> testfun(a, b)) => :c)
			@test slowtime.time > fasttime.time

		@test @select(df, cols(:c) = testfun(cols("a"), cols("b"))) == DataFrame(c = [2])

			fasttime = @timed @select(df, cols(:c) = testfun(cols("a"), cols("b")))
			slowtime = @timed select(df, [:a, :b] => ((a, b) -> testfun(a, b)) => :c)
			@test slowtime.time > fasttime.time

		@test @select(df, c = testdotfun.(:a, :b)) == DataFrame(c = [2])

			fasttime = @timed @select(df, c = testdotfun.(:a, :b))
			slowtime = @timed select(df, [:a, :b] => ((a, b) -> testdotfun.(a, b)) => :c)
			@test slowtime.time > fasttime.time

		@test @select(df, cols(:c) = testdotfun.(:a, :b)) == DataFrame(c = [2])

			fasttime = @timed @select(df, cols(:c) = testdotfun.(:a, :b))
			slowtime = @timed select(df, [:a, :b] => ((a, b) -> testdotfun.(a, b)) => :c)
			@test slowtime.time > fasttime.time

		@test @select(df, c = testdotfun.(cols("a"), cols("b"))) == DataFrame(c = [2])

			fasttime = @timed @select(df, c = testdotfun.(cols("a"), cols("b")))
			slowtime = @timed select(df, [:a, :b] => ((a, b) -> testdotfun.(a, b)) => :c)
			@test slowtime.time > fasttime.time

		@test @select(df, cols(:c) = testdotfun.(cols("a"), cols("b"))) == DataFrame(c = [2])

			fasttime = @timed @select(df, cols(:c) = testdotfun.(cols("a"), cols("b")))
			slowtime = @timed select(df, [:a, :b] => ((a, b) -> testdotfun.(a, b)) => :c)
			@test slowtime.time > fasttime.time

		gd = groupby(df, :a)

		@test @combine(gd, testnt(:b)) == DataFrame(a = [1], c = [2])

			fasttime = @timed @combine(gd, testnt(:b))
			slowtime = @timed combine(gd, :b => (b -> testnt(b)) => AsTable)
			@test slowtime.time > fasttime.time
	end
end
end # module