
module TestDataFrames

using Test
using DataFrames
using DataFramesMeta
using Statistics

const ≅ = isequal

@testset "@transform" begin
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

    n_str = "new_column"
    n_sym = :new_column
    n_space = "new column"

    @test @transform(df, :n = :i).n == df.i
    @test @transform(df, :n = :i .+ :g).n == df.i .+ df.g
    @test @transform(df, :n = :t .* string.(:y)).n == df.t .* string.(df.y)
    @test @transform(df, :n = Symbol.(:y, ^(:t))).n == Symbol.(df.y, :t)
    @test @transform(df, :n = Symbol.(:y, ^(:body))).n == Symbol.(df.y, :body)
    @test @transform(df, :body = :i).body == df.i
    @test @transform(df, :transform = :i).transform == df.i

    @test @transform(df, :n = $iq).n == df.i
    @test @transform(df, :n = $iq .+ $gq).n == df.i .+ df.g
    @test @transform(df, :n = $tq .* string.($yq)).n == df.t .* string.(df.y)
    @test @transform(df, :n = Symbol.($yq, ^(:t))).n == Symbol.(df.y, :t)
    @test @transform(df, :n = Symbol.($yq, ^(:body))).n == Symbol.(df.y, :body)
    @test @transform(df, :body = $iq).body == df.i
    @test @transform(df, :transform = $iq).transform == df.i

    @test @transform(df, :n = $ir).n == df.i
    @test @transform(df, :n = $ir .+ $gr).n == df.i .+ df.g
    @test @transform(df, :n = $tr .* string.($yr)).n == df.t .* string.(df.y)
    @test @transform(df, :n = Symbol.($yr, ^(:t))).n == Symbol.(df.y, :t)
    @test @transform(df, :n = Symbol.($yr, ^(:body))).n == Symbol.(df.y, :body)
    @test @transform(df, :body = $ir).body == df.i
    @test @transform(df, :transform = $ir).transform == df.i
    @test @transform(df, :n = $"g" + $:i).n == df.g + df.i
    @test @transform(df, :n = $1 + $2).n == df.g + df.i

    @test @transform(df, :n = :i).g !== df.g

    newdf = @transform(df, :n = :i)
    @test newdf[:, Not(:n)] ≅ df

    @test @transform(df, :i) ≅ df
    @test @transform(df, :i, :g) ≅ df

    @test @transform(df, $"new_column" = :i).new_column == df.i
    @test @transform(df, $n_str = :i).new_column == df.i
    @test @transform(df, $n_str = $"i" .+ 0).new_column == df.i
    @test @transform(df, $n_sym  = :i).new_column == df.i
    @test @transform(df, $n_space = :i)."new column" == df.i
    @test @transform(df, $("new" * "_" * "column") = :i).new_column == df.i

    @test @transform(df, :n = 1).n == fill(1, nrow(df))

    @test @transform(df, :n = :i .* :g).n == [1, 2, 3, 8, 10]
end

@testset "@transform with :block" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing]
        )

    d = @transform df begin
        :n1 = :i
        :n2 = :i .+ :g
    end
    @test d ≅ @transform(df, :n1 = :i, :n2 = :i .+ :g)

    d = @transform df begin
        $:n1 = :i
        :n2 = $:i .+ :g
    end
    @test d ≅ @transform(df, :n1 = :i, :n2 = :i .+ :g)

    d = @transform df begin
        :n1 = $:i
        $:n2 = :i .+ :g
    end
    @test d ≅ @transform(df, :n1 = :i, :n2 = :i .+ :g)

    d = @transform df begin
        :n1 = begin
            :i
        end
        :n2 = :i .+ :g
    end
    @test d ≅ @transform(df, :n1 = :i, :n2 = :i .+ :g)

    d = @transform df begin
        :n1 = @. :i * :g
        :n2 = @. :i * :g
    end
    @test d ≅ @transform(df, :n1 = :i .* :g, :n2 = :i .* :g)
end

@testset "@transform!" begin
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

    n_str = "new_column"
    n_sym = :new_column
    n_space = "new column"

    @test @transform!(df, :n = :i).n == df.i
    @test @transform!(df, :n = :i .+ :g).n == df.i .+ df.g
    @test @transform!(df, :n = :t .* string.(:y)).n == df.t .* string.(df.y)
    @test @transform!(df, :n = Symbol.(:y, ^(:t))).n == Symbol.(df.y, :t)
    @test @transform!(df, :n = Symbol.(:y, ^(:body))).n == Symbol.(df.y, :body)
    @test @transform!(df, :body = :i).body == df.i
    @test @transform!(df, :transform = :i).transform == df.i

    @test @transform!(df, :n = $iq).n == df.i
    @test @transform!(df, :n = $iq .+ $gq).n == df.i .+ df.g
    @test @transform!(df, :n = $tq .* string.($yq)).n == df.t .* string.(df.y)
    @test @transform!(df, :n = Symbol.($yq, ^(:t))).n == Symbol.(df.y, :t)
    @test @transform!(df, :n = Symbol.($yq, ^(:body))).n == Symbol.(df.y, :body)
    @test @transform!(df, :body = $iq).body == df.i
    @test @transform!(df, :transform = $iq).transform == df.i

    @test @transform!(df, :n = $ir).n == df.i
    @test @transform!(df, :n = $ir .+ $gr).n == df.i .+ df.g
    @test @transform!(df, :n = $tr .* string.($yr)).n == df.t .* string.(df.y)
    @test @transform!(df, :n = Symbol.($yr, ^(:t))).n == Symbol.(df.y, :t)
    @test @transform!(df, :n = Symbol.($yr, ^(:body))).n == Symbol.(df.y, :body)
    @test @transform!(df, :body = $ir).body == df.i
    @test @transform!(df, :transform = $ir).transform == df.i
    @test @transform!(df, :n = $"g" + $:i).n == df.g + df.i
    @test @transform!(df, :n = $1 + $2).n == df.g + df.i

    @test @transform!(df, $"new_column" = :i).new_column == df.i
    @test @transform!(df, $n_str = :i).new_column == df.i
    @test @transform(df, $n_str = $"i" .+ 0).new_column == df.i
    @test @transform!(df, $n_sym  = :i).new_column == df.i
    @test @transform!(df, $n_space = :i)."new column" == df.i
    @test @transform!(df, $("new" * "_" * "column") = :i).new_column == df.i

    @test @transform!(df, :n = 1).n == fill(1, nrow(df))
    @test @transform!(df, :n = :i .* :g).n == [1, 2, 3, 8, 10]

    # non-copying
    @test @transform!(df, :n = :i).g === df.g
    # mutating
    df2 = copy(df)
    @test @transform!(df, :i) === df
    @test df ≅ df2
    @test @transform!(df, :i, :g) ≅ df2
    @transform!(df, :n2 = :i)
    @test df[:, Not(:n2)] ≅ df2
end

@testset "@transform! with :block" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing]
        )

    d = @transform! df begin
        :n1 = :i
        :n2 = :i .+ :g
    end
    @test d ≅ @transform!(df, :n1 = :i, :n2 = :i .+ :g)

    d = @transform! df begin
        $:n1 = :i
        :n2 = $:i .+ :g
    end
    @test d ≅ @transform!(df, :n1 = :i, :n2 = :i .+ :g)

    d = @transform df begin
        :n1 = $:i
        :n2 = $:n2 = :i .+ :g
    end
    @test d ≅ @transform!(df, :n1 = :i, :n2 = :i .+ :g)

    d = @transform! df begin
        :n1 = begin
            :i
        end
        :n2 = :i .+ :g
    end
    @test d ≅ @transform!(df, :n1 = :i, :n2 = :i .+ :g)

    d = @transform! df begin
        :n1 = @. :i * :g
        :n2 = @. :i * :g
    end
    @test d ≅ @transform!(df, :n1 = :i .* :g, :n2 = :i .* :g)
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

s = [:i, :g]

@testset "limits of @transform" begin
    ## Test for not-implemented or strange behavior
    # @test_throws throws a `LoadError` when it
    # should throw an `ArgumentError`. Regardless,
    # the following should error so these tests are
    # left in.
    #
    # This behavior is part of `@test_throws` and
    # not part of DataFramesMeta.
    @test_throws LoadError @eval @transform(df, [:i, :g])
    @test_throws LoadError @eval @transform(df, All())
    @test_throws LoadError @eval @transform(df, Between(:i, :t)).Between == df.i
    @test_throws LoadError @eval @transform(df, Not(:i)).Not == df.i
    @test_throws LoadError @eval @transform(df, Not([:i, :g]))
    @test_throws MethodError @eval @transform(df, :n = sum(Between(:i, :t)))
end

@testset "@select" begin
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

    n_str = "new_column"
    n_sym = :new_column
    n_space = "new column"

    @test @select(df, :i) == df[!, [:i]]
    @test @select(df, :i, :g) == df[!, [:i, :g]]
    df2 = copy(df)
    df2.n = df2.i .+ df2.g
    @test @select(df, :i, :g, :n = :i .+ :g) == df2[!, [:i, :g, :n]]

    @test @select(df, :n = :i).n == df.i
    @test @select(df, :n = :i .+ :g).n == df.i .+ df.g
    @test @select(df, :n = :t .* string.(:y)).n == df.t .* string.(df.y)
    @test @select(df, :n = Symbol.(:y, ^(:t))).n == Symbol.(df.y, :t)
    @test @select(df, :n = Symbol.(:y, ^(:body))).n == Symbol.(df.y, :body)
    @test @select(df, :body = :i).body == df.i
    @test @select(df, :transform = :i).transform == df.i

    @test @select(df, :n = $iq).n == df.i
    @test @select(df, :n = $iq .+ $gq).n == df.i .+ df.g
    @test @select(df, :n = $tq .* string.($yq)).n == df.t .* string.(df.y)
    @test @select(df, :n = Symbol.($yq, ^(:t))).n == Symbol.(df.y, :t)
    @test @select(df, :n = Symbol.($yq, ^(:body))).n == Symbol.(df.y, :body)
    @test @select(df, :body = $iq).body == df.i
    @test @select(df, :transform = $iq).transform == df.i

    @test @select(df, :n = $ir).n == df.i
    @test @select(df, :n = $ir .+ $gr).n == df.i .+ df.g
    @test @select(df, :n = $tr .* string.($yr)).n == df.t .* string.(df.y)
    @test @select(df, :n = Symbol.($yr, ^(:t))).n == Symbol.(df.y, :t)
    @test @select(df, :n = Symbol.($yr, ^(:body))).n == Symbol.(df.y, :body)
    @test @select(df, :body = $ir).body == df.i
    @test @select(df, :transform = $ir).transform == df.i
    @test @select(df, :n = $"g" + $:i).n == df.g + df.i
    @test @select(df, :n = $1 + $2).n == df.g + df.i

    @test @select(df, :n = 1).n == fill(1, nrow(df))

    @test @select(df, $"new_column" = :i).new_column == df.i
    @test @select(df, $n_str = :i).new_column == df.i
    @test @select(df, $n_str = $"i" .+ 0).new_column == df.i
    @test @select(df, $n_sym  = :i).new_column == df.i
    @test @select(df, $n_space = :i)."new column" == df.i
    @test @select(df, $("new" * "_" * "column") = :i).new_column == df.i

    @test @transform(df, :n = :i .* :g).n == [1, 2, 3, 8, 10]
end

@testset "select with :block" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing]
        )

    d = @select df begin
        :n1 = :i
        :n2 = :i .+ :g
    end
    @test d ≅ @select(df, :n1 = :i, :n2 = :i .+ :g)

    d = @select df begin
        $:n1 = :i
        :n2 = $:i .+ :g
    end
    @test d ≅ @select(df, :n1 = :i, :n2 = :i .+ :g)

    d = @select df begin
        :n1 = $:i
        $:n2 = :i .+ :g
    end
    @test d ≅ @select(df, :n1 = :i, :n2 = :i .+ :g)

    d = @select df begin
        :n1 = begin
            :i
        end
        :n2 = :i .+ :g
    end
    @test d ≅ @select(df, :n1 = :i, :n2 = :i .+ :g)

    d = @select df begin
        :n1 = @. :i * :g
        :n2 = @. :i * :g
    end
    @test d ≅ @select(df, :n1 = :i .* :g, :n2 = :i .* :g)
end

@testset "@select!" begin
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

    n_str = "new_column"
    n_sym = :new_column
    n_space = "new column"

    df2 = copy(df)
    df2.n = df2.i .+ df2.g

    @test @select!(copy(df), :i, :g, :n = :i .+ :g) == df2[!, [:i, :g, :n]]
    @test @select!(copy(df), :i, :g) == df2[!, [:i, :g]]
    @test @select!(copy(df), :i) == df2[!, [:i]]

    @test @select!(copy(df), :n = :i .+ :g).n == df.i .+ df.g
    @test @select!(copy(df), :n = :i).n == df.i
    @test @select!(copy(df), :n = :t .* string.(:y)).n == df.t .* string.(df.y)
    @test @select!(copy(df), :n = Symbol.(:y, ^(:t))).n == Symbol.(df.y, :t)
    @test @select!(copy(df), :n = Symbol.(:y, ^(:body))).n == Symbol.(df.y, :body)
    @test @select!(copy(df), :body = :i).body == df.i
    @test @select!(copy(df), :transform = :i).transform == df.i

    @test @select!(copy(df), :n = $iq).n == df.i
    @test @select!(copy(df), :n = $iq .+ $gq).n == df.i .+ df.g
    @test @select!(copy(df), :n = $tq .* string.($yq)).n == df.t .* string.(df.y)
    @test @select!(copy(df), :n = Symbol.($yq, ^(:t))).n == Symbol.(df.y, :t)
    @test @select!(copy(df), :n = Symbol.($yq, ^(:body))).n == Symbol.(df.y, :body)
    @test @select!(copy(df), :body = $iq).body == df.i
    @test @select!(copy(df), :transform = $iq).transform == df.i

    @test @select!(copy(df), :n = $ir).n == df.i
    @test @select!(copy(df), :n = $ir .+ $gr).n == df.i .+ df.g
    @test @select!(copy(df), :n = $tr .* string.($yr)).n == df.t .* string.(df.y)
    @test @select!(copy(df), :n = Symbol.($yr, ^(:t))).n == Symbol.(df.y, :t)
    @test @select!(copy(df), :n = Symbol.($yr, ^(:body))).n == Symbol.(df.y, :body)
    @test @select!(copy(df), :body = $ir).body == df.i
    @test @select!(copy(df), :transform = $ir).transform == df.i
    @test @select!(copy(df), :n = $"g" + $:i).n == df.g + df.i
    @test @select!(copy(df), :n = $1 + $2).n == df.g + df.i


    @test @select!(copy(df), :n = 1).n == fill(1, nrow(df))

    @test @select!(copy(df), $"new_column" = :i).new_column == df.i
    @test @select!(copy(df), $n_str = :i).new_column == df.i
    @test @select!(copy(df), $n_str = $:i .+ 0).new_column == df.i
    @test @select!(copy(df), $n_sym  = :i).new_column == df.i
    @test @select!(copy(df), $n_space = :i)."new column" == df.i
    @test @select!(copy(df), $("new" * "_" * "column") = :i).new_column == df.i

    # non-copying
    newcol = [1:5;]
    df2 = copy(df)
    df2.newcol = newcol
    @test @select!(df2, :newcol).newcol === newcol

    # mutating
    df2 = @select(df, :i)
    @test @select!(df, :i) === df
    @test df == df2
end

@testset "@select! with :block" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing]
        )

    d = @select! copy(df) begin
        :n1 = :i
        :n2 = :i .+ :g
    end
    @test d ≅ @select!(copy(df), :n1 = :i, :n2 = :i .+ :g)

    d = @select! copy(df) begin
        $:n1 = :i
        :n2 = $:i .+ :g
    end
    @test d ≅ @select!(copy(df), :n1 = :i, :n2 = :i .+ :g)

    d = @select! copy(df) begin
        :n1 = $:i
        $:n2 = :i .+ :g
    end
    @test d ≅ @select!(copy(df), :n1 = :i, :n2 = :i .+ :g)

    d = @select! copy(df) begin
        :n1 = begin
            :i
        end
        :n2 = :i .+ :g
    end
    @test d ≅ @select!(copy(df), :n1 = :i, :n2 = :i .+ :g)

    d = @select! copy(df) begin
        :n1 = @. :i * :g
        :n2 = @. :i * :g
    end
    @test d ≅ @select!(copy(df), :n1 = :i .* :g, :n2 = :i .* :g)
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

@testset "limits of @select" begin
    ## Test for not-implemented or strange behavior
    @test_throws LoadError @eval @select(df, [:i, :g])
    @test_throws LoadError @eval @select(df, All())
    @test_throws LoadError @eval @select(df, Between(:i, :t)).Between == df.i
    @test_throws LoadError @eval  @select(df, Not(:i)).Not == df.i
    @test_throws LoadError @eval @select(df, Not([:i, :g]))
    @test_throws MethodError @eval @select(df, :n = sum(Between(:i, :t)))
end

@testset "with" begin
    df = DataFrame(A = 1:3, B = [2, 1, 2])

    x = [2, 1, 0]

    @test  @with(df, :A .+ 1)   ==  df.A .+ 1
    @test  @with(df, :A .+ :B)  ==  df.A .+ df.B
    @test  @with(df, :A .+ x)   ==  df.A .+ x

    x = @with df begin
        res = 0.0
        for i in 1:length(:A)
            res += :A[i] * :B[i]
        end
        res
    end
    idx = :A
    @test  @with(df, $idx .+ :B)  ==  df.A .+ df.B
    idx2 = :B
    @test  @with(df, $idx .+ $idx2)  ==  df.A .+ df.B
    @test  @with(df, $:A .+ $"B")  ==  df.A .+ df.B

    @test_throws ArgumentError @with(df, :A + $2)

    @test  x == sum(df.A .* df.B)
    @test  @with(df, df[:A .> 1, ^([:B, :A])]) == df[df.A .> 1, [:B, :A]]
    @test  @with(df, DataFrame(a = :A * 2, b = :A .+ :B)) == DataFrame(a = df.A * 2, b = df.A .+ df.B)

    @test @with(df, :A) === df.A
    @test @with(df, $:A) === df.A
    @test @with(df, $"A") === df.A
end

@testset "orderby" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing]
        )

    @test @orderby(df, :c) ≅ df[[3, 1, 2, 4, 5], :]
    @test @orderby(df, -:g) ≅ df[[4, 5, 1, 2, 3], :]
    @test @orderby(df, :t) ≅ df[[1, 2, 3, 4, 5], :]

    @test @orderby(df, identity(:g), :g.^2) ≅ df[[1, 2, 3, 4, 5], :]

    subdf = @view df[1:3, :]

    @test @orderby(subdf, -:i) == df[[3, 2, 1], :]
end

@testset "orderby with :block" begin
    df = DataFrame(
        g = [1, 1, 1, 2, 2],
        i = 1:5,
        t = ["a", "b", "c", "c", "e"],
        y = [:v, :w, :x, :y, :z],
        c = [:g, :quote, :body, :transform, missing]
        )

    d = @orderby df begin
        :c
        :g .*  2
    end
    @test d ≅ @orderby(df, :c, :g .* 2)

    d = @orderby df begin
        $:c
        :g .*  2
    end
    @test d ≅ @orderby(df, :c, :g .* 2)

    d = @orderby df begin
        :c
        $:g .*  2
    end
    @test d ≅ @orderby(df, :c, :g .* 2)

    d = @orderby df begin
        begin
            :c
        end
        :g .*  2
    end
    @test d ≅ @orderby(df, :c, :g .* 2)

    d = @orderby df begin
        :c
        @. :g * 2
    end
    @test d ≅ @orderby(df, :c, :g .* 2)
end

@testset "cols with @select fix" begin
    df = DataFrame("X" => 1, "X Y Z" => 2)

    @test @select(df, $"X") == select(df, "X")
    @test @select(df, $"X Y Z") == select(df, "X Y Z")
    @test @transform(df, $"X") == df
    @test @transform(df, $"X Y Z") == df
end

macro linenums_macro(arg)
    if arg isa Expr && arg.head == :block && length(arg.args) == 1 && arg.args[1] isa LineNumberNode
        esc(:([true]))
    else
        esc(:([false]))
    end
end

macro linenums_macro_byrow(arg)
    if arg isa Expr && arg.head == :block && length(arg.args) == 1 && arg.args[1] isa LineNumberNode
        esc(:(true))
    else
        esc(:(false))
    end
end

@testset "removing lines" begin
    df = DataFrame(a = [1], b = [2])
    # Can't use @test because @test remove line numbers
    d = @transform(df, y = @linenums_macro begin end)
    @test d.y == [true]

    d = @transform df begin
        y = @linenums_macro begin end
    end

    @test d.y == [true]

    d = @transform df @byrow begin
        y = @linenums_macro_byrow begin end
    end

    @test d.y == [true]

    d = @subset(df, @linenums_macro begin end)

    @test nrow(d) == 1

    d = @subset df begin
        @byrow @linenums_macro_byrow begin end
    end

    @test nrow(d) == 1

    d = @subset df @byrow begin
        @linenums_macro_byrow begin end
    end

    @test nrow(d) == 1
end

end # module
