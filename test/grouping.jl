module TestGrouping

using Test
using DataFrames
using DataFramesMeta
using Statistics
using CategoricalArrays

const ≅ = isequal

d = DataFrame(n = 1:20, x = [3, 3, 3, 3, 1, 1, 1, 2, 1, 1, 2, 1, 1, 2, 2, 2, 3, 1, 1, 2])
g = groupby(d, :x, sort=true)

@test @combine(g, nsum = sum(:n)).nsum == [99, 84, 27]

@testset "@combine" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing]
        )

    m = [100, 200, 300, 400, 500]

    gq = :g
    iq = :i
    tq = :t
    yq = :y
    cq = :c

    gr = "g"
    ir = "i"
    tr = "t"
    yr = "y"
    cr = "c"

    gd = groupby(df, :g)

    n_str = "new_column"
    n_sym = :new_column
    n_space = "new column"

    @test @combine(gd, n = mean(:i)).n == [2.0, 4.5]
    @test @combine(gd, n = mean(:i) + mean(:g)).n == [3.0, 6.5]
    @test @combine(gd, n = first(:t .* string.(:y))).n == ["av", "cy"]
    @test @combine(gd, n = first(Symbol.(:y, syms(:t)))).n == [:vt, :yt]
    @test @combine(gd, n = first(Symbol.(:y, syms(:body)))).n == [:vbody, :ybody]
    @test @combine(gd, body = :i).body == df.i
    @test @combine(gd, transform = :i).transform == df.i
    @test @combine(gd, (n1 = [first(:i)], n2 = [first(:y)])).n1 == [1, 4]

    @test @combine(gd, n = mean(cols(iq))).n == [2.0, 4.5]
    @test @combine(gd, n = mean(cols(iq)) + mean(cols(gq))).n == [3.0, 6.5]
    @test @combine(gd, n = first(cols(tq) .* string.(cols(yq)))).n == ["av", "cy"]
    @test @combine(gd, n = first(Symbol.(cols(yq), syms(:t)))).n == [:vt, :yt]
    @test @combine(gd, n = first(Symbol.(cols(yq), syms(:body)))).n == [:vbody, :ybody]
    @test @combine(gd, body = cols(iq)).body == df.i
    @test @combine(gd, transform = cols(iq)).transform == df.i
    @test @combine(gd, (n1 = [first(cols(iq))], n2 = [first(cols(yq))])).n1 == [1, 4]

    @test @combine(gd, n = mean(cols(ir))).n == [2.0, 4.5]
    @test @combine(gd, n = mean(cols(ir)) + mean(cols(gr))).n == [3.0, 6.5]
    @test @combine(gd, n = first(cols(tr) .* string.(cols(yr)))).n == ["av", "cy"]
    @test @combine(gd, n = first(Symbol.(cols(yr), syms(:t)))).n == [:vt, :yt]
    @test @combine(gd, n = first(Symbol.(cols(yr), syms(:body)))).n == [:vbody, :ybody]
    @test @combine(gd, body = cols(ir)).body == df.i
    @test @combine(gd, transform = cols(ir)).transform == df.i
    @test @combine(gd, (n1 = [first(cols(ir))], n2 = [first(cols(yr))])).n1 == [1, 4]
    @test @combine(gd, n = mean(cols("i")) + 0 * first(cols(:g))).n == [2.0, 4.5]
    @test @combine(gd, n = mean(cols(2)) + first(cols(1))).n == [3.0, 6.5]


    @test @combine(gd, :i) == select(df, :g, :i)
    @test @combine(gd, :i, :g) ≅ select(df, :g, :i)

    @test @combine(gd, :i, n = 1).n == fill(1, nrow(df))

    @test @combine(gd, cols("new_column") = 2).new_column == [2, 2]
    @test @combine(gd, cols(n_str) = 2).new_column == [2, 2]
    @test @combine(gd, cols(n_sym) = 2).new_column == [2, 2]
    @test @combine(gd, cols(n_space) = 2)."new column" == [2, 2]
    @test @combine(gd, cols("new" * "_" * "column") = 2)."new_column" == [2, 2]
end

# Defined outside of `@testset` due to use of `@eval`
df = DataFrame(
    g = [1, 1, 1, 2, 2],
    i = 1:5,
    t = ["a", "b", "c", "c", "e"],
    y = [:v, :w, :x, :y, :z],
    c = [:g, :quote, :body, :transform, missing]
    )

m = [100, 200, 300, 400, 500]

gq = :g
iq = :i
tq = :t
yq = :y
cq = :c

gr = "g"
ir = "i"
tr = "t"
yr = "y"
cr = "c"

gd = groupby(df, :g)

newvar = :n

@testset "Limits of @combine" begin
    if DataFramesMeta.DATAFRAMES_GEQ_22
        @test_throws ArgumentError @combine(gd, [:i, :g])
        @test_throws ArgumentError @combine(gd, All()).function isa Vector{<:All}
        @test_throws ArgumentError @combine(gd, Not(:i)).i_function isa Vector{<:InvertedIndex}
        @test_throws ArgumentError @combine(gd, Not([:i, :g])).g == [1, 2]
    else
        t = @combine(gd, [:i, :g])[!, 2]
        @test t == [[1, 2, 3], [1, 1, 1], [4, 5], [2, 2]]
        @test t isa Vector{SubArray{Int64,1,Array{Int64,1},Tuple{Array{Int64,1}},false}}
        @test @combine(gd, All()).function isa Vector{<:All}
        @test @combine(gd, Not(:i)).i_function isa Vector{<:InvertedIndex}
        @test @combine(gd, Not([:i, :g])).g == [1, 2]
    end
    @test_throws MethodError @eval @combine(gd, n = sum(Between(:i, :t)))
    @test_throws LoadError @eval @combine(gd; n = mean(:i))
    @test_throws ArgumentError @eval @combine(gd, n = mean(:i) + mean(cols(1)))
end

@testset "@by" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing]
        )

    m = [100, 200, 300, 400, 500]

    gq = :g
    iq = :i
    tq = :t
    yq = :y
    cq = :c

    gr = "g"
    ir = "i"
    tr = "t"
    yr = "y"
    cr = "c"

    gd = groupby(df, :g)

    n_str = "new_column"
    n_sym = :new_column
    n_space = "new column"

    @test @by(df, :g, n = mean(:i)).n == [2.0, 4.5]
    @test @by(df, :g, n = mean(:i) + mean(:g)).n == [3.0, 6.5]
    @test @by(df, :g, n = first(:t .* string.(:y))).n == ["av", "cy"]
    @test @by(df, :g, n = first(Symbol.(:y, syms(:t)))).n == [:vt, :yt]
    @test @by(df, :g, n = first(Symbol.(:y, syms(:body)))).n == [:vbody, :ybody]
    @test @by(df, :g, body = :i).body == df.i
    @test @by(df, :g, transform = :i).transform == df.i
    @test @by(df, :g, (n1 = [first(:i)], n2 = [first(:y)])).n1 == [1, 4]

    @test @by(df, :g, n = mean(cols(iq))).n == [2.0, 4.5]
    @test @by(df, :g, n = mean(cols(iq)) + mean(cols(gq))).n == [3.0, 6.5]
    @test @by(df, :g, n = first(cols(tq) .* string.(cols(yq)))).n == ["av", "cy"]
    @test @by(df, :g, n = first(Symbol.(cols(yq), syms(:t)))).n == [:vt, :yt]
    @test @by(df, :g, n = first(Symbol.(cols(yq), syms(:body)))).n == [:vbody, :ybody]
    @test @by(df, :g, body = cols(iq)).body == df.i
    @test @by(df, :g, transform = cols(iq)).transform == df.i
    @test @by(df, :g, (n1 = [first(cols(iq))], n2 = [first(cols(yq))])).n1 == [1, 4]

    @test @by(df, "g", n = mean(cols(ir))).n == [2.0, 4.5]
    @test @by(df, "g", n = mean(cols(ir)) + mean(cols(gr))).n == [3.0, 6.5]
    @test @by(df, "g", n = first(cols(tr) .* string.(cols(yr)))).n == ["av", "cy"]
    @test @by(df, "g", n = first(Symbol.(cols(yr), syms(:t)))).n == [:vt, :yt]
    @test @by(df, "g", n = first(Symbol.(cols(yr), syms(:body)))).n == [:vbody, :ybody]
    @test @by(df, "g", body = cols(ir)).body == df.i
    @test @by(df, "g", transform = cols(ir)).transform == df.i
    @test @by(df, "g", (n1 = [first(cols(ir))], n2 = [first(cols(yr))])).n1 == [1, 4]
    @test @by(df, "g", n = mean(cols("i")) + 0 * first(cols(:g))).n == [2.0, 4.5]
    @test @by(df, "g", n = mean(cols(2)) + first(cols(1))).n == [3.0, 6.5]


    @test @by(df, :g, :i) == select(df, :g, :i)
    @test @by(df, :g, :i, :g) ≅ select(df, :g, :i)

    @test @by(df, :g, :i, n = 1).n == fill(1, nrow(df))

    @test @by(df, :g, cols("new_column") = 2).new_column == [2, 2]
    @test @by(df, :g, cols(n_str) = 2).new_column == [2, 2]
    @test @by(df, :g, cols(n_sym) = 2).new_column == [2, 2]
    @test @by(df, :g, cols(n_space) = 2)."new column" == [2, 2]
    @test @by(df, :g, cols("new" * "_" * "column") = 2)."new_column" == [2, 2]
end

# Defined outside of `@testset` due to use of `@eval`
df = DataFrame(
    g = [1, 1, 1, 2, 2],
    i = 1:5,
    t = ["a", "b", "c", "c", "e"],
    y = [:v, :w, :x, :y, :z],
    c = [:g, :quote, :body, :transform, missing]
    )

m = [100, 200, 300, 400, 500]

gq = :g
iq = :i
tq = :t
yq = :y
cq = :c

gr = "g"
ir = "i"
tr = "t"
yr = "y"
cr = "c"

gd = groupby(df, :g)

newvar = :n

@testset "limits of @by" begin
    if DataFramesMeta.DATAFRAMES_GEQ_22
        @test_throws ArgumentError @by(df, :g, [:i, :g])
        @test_throws ArgumentError @by(df, :g, All()).function isa Vector{<:All}
        @test_throws ArgumentError @by(df, :g, Not(:i)).i_function isa Vector{<:InvertedIndex}
        @test_throws ArgumentError @by(df, :g, Not([:i, :g])).g == [1, 2]
    else
        t = @by(df, :g, [:i, :g])[!, 2]
        @test t == [[1, 2, 3], [1, 1, 1], [4, 5], [2, 2]]
        @test t isa Vector{SubArray{Int64,1,Array{Int64,1},Tuple{Array{Int64,1}},false}}
        @test @by(df, :g, All()).function isa Vector{<:All}
        @test @by(df, :g, Not(:i)).i_function isa Vector{<:InvertedIndex}
        @test @by(df, :g, Not([:i, :g])).g == [1, 2]
    end
    @test_throws MethodError @eval @by(df, :g, n = sum(Between(:i, :t)))
    @test_throws MethodError @eval @by(df, :g; n = mean(:i))
    @test_throws ArgumentError @eval @by(df, :g, n = mean(:i) + mean(cols(1)))
end

@testset "@transform with grouped data frame" begin
	d = DataFrame(n = 1:20, x = [3, 3, 3, 3, 1, 1, 1, 2, 1, 1, 2, 1, 1, 2, 2, 2, 3, 1, 1, 2])
	g = groupby(d, :x)

	@test (@transform(g, y = :n .- median(:n)))[1,:y] == -2.0

	d = DataFrame(a = [1,1,1,2,2,3,3,1],
	              b = Any[1,2,3,missing,missing,6.0,5.0,4],
	              c = CategoricalArray([1,2,3,1,2,3,1,2]))
	g = groupby(d, :a)

      ## Scalar output
	# Type promotion Int -> Float
	t = @transform(g, t = :b[1]).t
	s = @select(g, t = :b[1]).t
	@test t ≅ s ≅ [1.0, 1.0, 1.0, missing, missing, 6.0, 6.0, 1.0] &&
	      t isa Vector{Union{Float64, Missing}}

	# Type promotion Number -> Any
	t = @transform(g, t = isequal(:b[1], 1) ? :b[1] : "a").t
	s = @select(g, t = isequal(:b[1], 1) ? :b[1] : "a").t
	@test t ≅ s ≅ [1, 1, 1, "a", "a", "a", "a", 1] &&
	      t isa Vector{Any}
	## Vector output
	# Normal use
	t = @transform(g, t = :b .- mean(:b)).t
	s = @select(g, t = :b .- mean(:b)).t
	@test t ≅ s ≅ [-1.5, -0.5, 0.5, missing, missing, 0.5, -0.5, 1.5] &&
	      t isa Vector{Union{Float64, Missing}}
	# Type promotion
	t = @transform(g, t = isequal(:b[1], 1) ? fill(1, length(:b)) : fill(2.0, length(:b))).t
	s = @transform(g, t = isequal(:b[1], 1) ? fill(1, length(:b)) : fill(2.0, length(:b))).t
	@test t ≅ s ≅ [1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 2.0, 1.0] &&
	      t isa Vector{Float64}
	# Vectors whose eltypes promote to any
	t = @transform(g, t = isequal(:b[1], 1) ? :b : fill("a", length(:b))).t
	s = @transform(g, t = isequal(:b[1], 1) ? :b : fill("a", length(:b))).t
	@test s ≅ t ≅ [1, 2, 3, "a", "a", "a", "a", 4] &&
	      t isa Vector{Any}
	# Categorical Array
	# Scalar
	t = @transform(g, t = :c[1]).t
	s = @transform(g, t = :c[1]).t
	@test t ≅ s ≅  [1, 1, 1, 1, 1, 3, 3, 1] &&
	      t isa CategoricalVector{Int}
	# Vector
	t = @transform(g, t = :c).t
	s = @transform(g, t = :c).t
	@test t ≅ s ≅ [1, 2, 3, 1, 2, 3, 1, 2] &&
	      t isa CategoricalVector{Int}

	@test @transform(g, t = :c).a ≅ d.a
	@test @select(g, :a, t = :c).a ≅ d.a
end

@testset "@where with a grouped data frame" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing]
    )

    gd = groupby(df, :g)

    @test @where(gd, :i .== first(:i)) ≅ df[[1, 4], :]
    @test @where(gd, cols(:i) .> mean(cols(:i)), :t .== "c") ≅ df[[3], :]
    @test @where(gd, :c .== :g) ≅ df[[], :]
end
end # module
