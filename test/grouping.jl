module TestGrouping

using Test
using DataFrames
using DataFramesMeta
using Statistics
using Tables

const ≅ = isequal

d = DataFrame(n = 1:20, x = [3, 3, 3, 3, 1, 1, 1, 2, 1, 1, 2, 1, 1, 2, 2, 2, 3, 1, 1, 2])
g = groupby(d, :x, sort=true)

@test  @where(d, :x .== 3) == DataFramesMeta.where(d, x -> x.x .== 3)
@test  DataFrame(@where(g, length(:x) > 5)) == DataFrame(DataFramesMeta.where(g, x -> length(x.x) > 5))
@test  DataFrame(@where(g, length(:x) > 5))[!, :n][1:3] == [5, 6, 7]

@test  DataFrame(DataFramesMeta.orderby(g, x -> mean(x.n))) == DataFrame(@orderby(g, mean(:n)))

@test @based_on(g, nsum = sum(:n)).nsum == [99, 84, 27]


@testset "@based_on" begin
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

    @test @based_on(gd, n = mean(:i)).n == [2.0, 4.5]
    @test @based_on(gd, n = mean(:i) + mean(:g)).n == [3.0, 6.5]
    @test @based_on(gd, n = first(:t .* string.(:y))).n == ["av", "cy"]
    @test @based_on(gd, n = first(Symbol.(:y, ^(:t)))).n == [:vt, :yt]
    @test @based_on(gd, n = first(Symbol.(:y, ^(:body)))).n == [:vbody, :ybody]
    @test @based_on(gd, body = :i).body == df.i
    @test @based_on(gd, transform = :i).transform == df.i
    @test @based_on(gd, (n1 = [first(:i)], n2 = [first(:y)])).n1 == [1, 4]

    @test @based_on(gd, n = mean(cols(iq))).n == [2.0, 4.5]
    @test @based_on(gd, n = mean(cols(iq)) + mean(cols(gq))).n == [3.0, 6.5]
    @test @based_on(gd, n = first(cols(tq) .* string.(cols(yq)))).n == ["av", "cy"]
    @test @based_on(gd, n = first(Symbol.(cols(yq), ^(:t)))).n == [:vt, :yt]
    @test @based_on(gd, n = first(Symbol.(cols(yq), ^(:body)))).n == [:vbody, :ybody]
    @test @based_on(gd, body = cols(iq)).body == df.i
    @test @based_on(gd, transform = cols(iq)).transform == df.i
    @test @based_on(gd, (n1 = [first(cols(iq))], n2 = [first(cols(yq))])).n1 == [1, 4]

    @test @based_on(gd, n = mean(cols(ir))).n == [2.0, 4.5]
    @test @based_on(gd, n = mean(cols(ir)) + mean(cols(gr))).n == [3.0, 6.5]
    @test @based_on(gd, n = first(cols(tr) .* string.(cols(yr)))).n == ["av", "cy"]
    @test @based_on(gd, n = first(Symbol.(cols(yr), ^(:t)))).n == [:vt, :yt]
    @test @based_on(gd, n = first(Symbol.(cols(yr), ^(:body)))).n == [:vbody, :ybody]
    @test @based_on(gd, body = cols(ir)).body == df.i
    @test @based_on(gd, transform = cols(ir)).transform == df.i
    @test @based_on(gd, (n1 = [first(cols(ir))], n2 = [first(cols(yr))])).n1 == [1, 4]

    @test @based_on(gd, :i) == select(df, :g, :i)
    @test @based_on(gd, :i, :g) ≅ select(df, :g, :i)

    @test @based_on(gd, :i, n = 1).n == fill(1, nrow(df))
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

@testset "Limits of @based_on" begin
    t = @based_on(gd, [:i, :g]).i_g_function
    @test t == [[1, 2, 3], [1, 1, 1], [4, 5], [2, 2]]
    @test t isa Vector{SubArray{Int64,1,Array{Int64,1},Tuple{Array{Int64,1}},false}}
    @test @based_on(gd, All()).function isa Vector{<:All}
    @test @based_on(gd, Not(:i)).i_function isa Vector{<:InvertedIndex}
    @test @based_on(gd, Not([:i, :g])).g == [1, 2]
    @test_throws ArgumentError @eval @based_on(gd, cols(newvar) = mean(:i))
    @test_throws MethodError @eval @based_on(gd, n = sum(Between(:i, :t)))
    @test_throws LoadError @eval @based_on(gd; n = mean(:i))
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

    newvar = :n
    @test @by(df, :g, n = mean(:i)).n == [2.0, 4.5]
    @test @by(df, :g, n = mean(:i) + mean(:g)).n == [3.0, 6.5]
    @test @by(df, :g, n = first(:t .* string.(:y))).n == ["av", "cy"]
    @test @by(df, :g, n = first(Symbol.(:y, ^(:t)))).n == [:vt, :yt]
    @test @by(df, :g, n = first(Symbol.(:y, ^(:body)))).n == [:vbody, :ybody]
    @test @by(df, :g, body = :i).body == df.i
    @test @by(df, :g, transform = :i).transform == df.i
    @test @by(df, :g, (n1 = [first(:i)], n2 = [first(:y)])).n1 == [1, 4]

    @test @by(df, :g, n = mean(cols(iq))).n == [2.0, 4.5]
    @test @by(df, :g, n = mean(cols(iq)) + mean(cols(gq))).n == [3.0, 6.5]
    @test @by(df, :g, n = first(cols(tq) .* string.(cols(yq)))).n == ["av", "cy"]
    @test @by(df, :g, n = first(Symbol.(cols(yq), ^(:t)))).n == [:vt, :yt]
    @test @by(df, :g, n = first(Symbol.(cols(yq), ^(:body)))).n == [:vbody, :ybody]
    @test @by(df, :g, body = cols(iq)).body == df.i
    @test @by(df, :g, transform = cols(iq)).transform == df.i
    @test @by(df, :g, (n1 = [first(cols(iq))], n2 = [first(cols(yq))])).n1 == [1, 4]

    @test @by(df, "g", n = mean(cols(ir))).n == [2.0, 4.5]
    @test @by(df, "g", n = mean(cols(ir)) + mean(cols(gr))).n == [3.0, 6.5]
    @test @by(df, "g", n = first(cols(tr) .* string.(cols(yr)))).n == ["av", "cy"]
    @test @by(df, "g", n = first(Symbol.(cols(yr), ^(:t)))).n == [:vt, :yt]
    @test @by(df, "g", n = first(Symbol.(cols(yr), ^(:body)))).n == [:vbody, :ybody]
    @test @by(df, "g", body = cols(ir)).body == df.i
    @test @by(df, "g", transform = cols(ir)).transform == df.i
    @test @by(df, "g", (n1 = [first(cols(ir))], n2 = [first(cols(yr))])).n1 == [1, 4]

    @test @by(df, :g, :i) == select(df, :g, :i)
    @test @by(df, :g, :i, :g) ≅ select(df, :g, :i)

    @test @by(df, :g, :i, n = 1).n == fill(1, nrow(df))
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
    t = @by(df, :g, [:i, :g]).i_g_function
    @test t == [[1, 2, 3], [1, 1, 1], [4, 5], [2, 2]]
    @test t isa Vector{SubArray{Int64,1,Array{Int64,1},Tuple{Array{Int64,1}},false}}
    @test @by(df, :g, All()).function isa Vector{<:All}
    @test @by(df, :g, Not(:i)).i_function isa Vector{<:InvertedIndex}
    @test @by(df, :g, Not([:i, :g])).g == [1, 2]
    @test_throws ArgumentError @eval @by(df, :g, cols(newvar) = mean(:i))
    @test_throws MethodError @eval @by(df, :g, n = sum(Between(:i, :t)))
    @test_throws MethodError @eval @by(df, :g; n = mean(:i))
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

    @test @transform(g, t = :c).a ≅ df.a
    @test @select(g, :a, t = :c).a ≅ df.a
end

end # module
