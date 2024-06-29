module TestKW

using Test
using DataFrames
using DataFramesMeta
using Statistics

const ≅ = isequal

df = DataFrame(a = [1, 1, 2, 2], b = [3, 4, 5, missing])
gd = groupby(df, :a)

# @subset
# skipmissing, view, ungroup
@testset "@subset keyword" begin
    correct = view(df, df.a .== 1, :)
    df2 = @subset(df, :a .== 1; view = true)
    @test df2 ≅ correct

    df2 = @subset df begin
        :a .== 1
        @kwarg view = true
    end
    @test df2 ≅ correct

    @test_throws ArgumentError @subset(df, :b .== 3; skipmissing = false)

    correct = gd
    gd2 = @subset(gd, fill(true, length(:a)); ungroup = false)
    @test gd2 ≅ correct

    gd2 = @subset gd begin
        fill(true, length(:a))
        @kwarg ungroup = false
    end
    @test gd2 ≅ correct
end

# @rsubset
# skipmissing, view, ungroup
@testset "@rsubset keyword" begin
    correct = view(df, df.a .== 1, :)

    df2 = @rsubset(df, :a == 1; view = true)
    @test df2 ≅ view(df, df.a .== 1, :)

    df2 = @rsubset df begin
        :a == 1
        @kwarg view = true
    end
    @test df2 ≅ correct

    @test_throws ArgumentError @rsubset(df, :b == 3; skipmissing = false)

    correct = gd
    gd2 = @rsubset(gd, first(true); ungroup = false)
    @test gd2 ≅ correct

    gd2 = @rsubset gd begin
        first(true);
        @kwarg ungroup = false
    end
    @test gd ≅ correct
end

# @subset!
# skipmissing, ungroup
@testset "@subset! keyword" begin
    @test_throws ArgumentError @subset!(copy(df), :b .== 3; skipmissing = false)

    correct = gd

    gd2 = @subset!(deepcopy(gd), [true, true]; ungroup = false)
    @test gd2 ≅ correct

    gd2 = @subset! deepcopy(gd) begin
        [true, true]
        @kwarg ungroup = false
    end
    @test gd2 ≅ correct
end

# @rsubset!
# skipmissing, ungroup
@testset "@rsubset! keyword" begin
    @test_throws ArgumentError @rsubset!(copy(df), :b == 3; skipmissing = false)

    correct = gd

    gd2 = @rsubset!(deepcopy(gd), first(true); ungroup = false)
    @test gd2 ≅ correct

    gd2 = @rsubset! deepcopy(gd) begin
        first(true)
        @kwarg ungroup = false
    end
    @test gd2 ≅ correct
end

# @orderby # Not added
@test_throws MethodError @orderby(df, :a; view = true)

# @rorderby # Not added
@test_throws MethodError @rorderby(df, :a; view = true)

# @select
# copycols, renamecols (not relevant)
# keepkeys, ungroup
@testset "@select keyword" begin
    correct = DataFrame(a = df.a; copycols = false)

    df2 = @select(df, :a; copycols = false)
    @test (df2 ≅ correct && (df2.a === correct.a))

    df2 = @select df begin
        :a
        @kwarg copycols = false
    end
    @test (df2 ≅ correct && (df2.a === correct.a))

    correct = df
    df2 = @select(gd, :b; keepkeys = true)
    @test df2 ≅ correct

    df2 = @select gd begin
        :b
        @kwarg keepkeys = true
    end
    @test df2 ≅ correct

    correct = gd

    gd2 = @select(gd, :b; ungroup = false)
    gd2 = @select gd begin
        :b
        @kwarg ungroup = false
    end
    @test gd2 ≅ correct
end

# @rselect
# copycols, renamecols (not relevant)
# keepkeys, ungroup
@testset "@rselect keyword" begin
    correct = DataFrame(a = df.a; copycols = false)

    df2 = @rselect(df, :a; copycols = false)
    @test (df2 ≅ correct && (df2.a === correct.a))

    df2 = @rselect df begin
        :a
        @kwarg copycols = false
    end
    @test (df2 ≅ correct && (df2.a === correct.a))

    correct = df
    df2 = @rselect(gd, :b; keepkeys = true)
    @test df2 ≅ correct

    df2 = @rselect gd begin
        :b
        @kwarg keepkeys = true
    end
    @test df2 ≅ correct

    correct = gd

    gd2 = @rselect(gd, :b; ungroup = false)
    gd2 = @rselect gd begin
        :b
        @kwarg ungroup = false
    end
    @test gd2 ≅ correct
end

# @select!
# renamecols (not relevant), ungroup
@testset "@select! keyword" begin
    correct = gd

    gd2 = @select!(deepcopy(gd), :b; ungroup = false)
    @test gd2 ≅ correct

    @select! deepcopy(gd) begin
        :b
        @kwarg ungroup = false
    end

    @test gd2 ≅ correct
end

# @rselect!
# renamecols (not relevant), ungroup
@testset "@rselect! keyword" begin
    correct = gd

    gd2 = @rselect!(deepcopy(gd), :b; ungroup = false)
    @test gd2 ≅ correct

    @rselect! deepcopy(gd) begin
        :b
        @kwarg ungroup = false
    end

    @test gd2 ≅ correct
end

# @transform
# copycols, renamecols (not relevant)
# ungroup
@testset "@transform keyword" begin
    correct = df.b

    df2 = @transform(df, :a; copycols = false)
    @test df2 ≅ df
    # The :a above counts as a transformation, and
    # is thus copied
    @test df2.b === correct

    df2 = @transform df begin
        :a
        @kwarg copycols = false
    end
    @test df2.b === correct

    correct = gd

    gd2 = @transform(gd, :b; ungroup = false)
    @test gd2 ≅ correct

    gd2 = @transform gd begin
        :b
        @kwarg ungroup = false
    end
    @test gd2 ≅ correct
end

# @rtransform
# copycols, renamecols (not relevant)
# ungroup
@testset "@rtransform keyword" begin
    correct = df.b

    df2 = @rtransform(df, :a; copycols = false)
    @test df2.b === correct

    df2 = @rtransform df begin
        :a
        @kwarg copycols = false
    end
    @test df2.b === correct

    correct = gd

    gd2 = @rtransform(gd, :b; ungroup = false)
    @test gd2 ≅ correct

    gd2 = @rtransform gd begin
        :b
        @kwarg ungroup = false
    end
    @test gd2 ≅ correct
end

# @transform!
# renamecols (not relevant), ungroup
@testset "@transform! keyword" begin
    correct = df.a

    correct = gd

    gd2 = @transform(deepcopy(gd), :b; ungroup = false)
    @test gd2 ≅ correct

    gd2 = @transform deepcopy(gd) begin
        :b
        @kwarg ungroup = false
    end
    @test gd2 ≅ correct
end


# @rtransform!
# renamecols (not relevant), ungroup
@testset "@rtransform! keyword" begin
    correct = df.a

    correct = gd

    gd2 = @rtransform(deepcopy(gd), :b; ungroup = false)
    @test gd2 ≅ correct

    gd2 = @rtransform deepcopy(gd) begin
        :b
        @kwarg ungroup = false
    end
    @test gd2 ≅ correct
end

# @combine
# renamecols (not relevant), keepkeys,
# ungroup
@testset "@combine keyword" begin
    correct = DataFrame(a = [1, 2], b_f = [3, 5])

    df2 = @combine(gd, :b_f = first(:b); keepkeys = true)
    @test sort(df2, :a) ≅ correct

    df2 = @combine gd begin
        :b_f = first(:b)
        @kwarg keepkeys = true
    end
    @test sort(df2, :a) ≅ correct

    correct = gd

    gd2 = @combine(gd, :b = :b; ungroup = false)
    @test gd2 ≅ correct

    gd2 = @combine gd begin
        :b = :b
        @kwarg ungroup = false
    end
    @test gd2 ≅ correct
end


# @by
# renamecols (not relevant), keepkeys,
# ungroup
@testset "@combine keyword" begin
    correct = DataFrame(a = [1, 2], b_f = [3, 5])

    df2 = @by(df, :a, :b_f = first(:b); keepkeys = true)
    @test sort(df2, :a) ≅ correct

    df2 = @by df :a begin
        :b_f = first(:b)
        @kwarg keepkeys = true
    end
    @test sort(df2, :a) ≅ correct

    correct = gd

    gd2 = @by(df, :a, :b = :b; ungroup = false)
    @test gd2 ≅ correct

    gd2 = @by df :a begin
        :b = :b
        @kwarg ungroup = false
    end
    @test gd2 ≅ correct
end

@testset "Pairs and keyword arguments" begin
    correct = @view df[df.a .== 1, :]

    t = [:view => true]
    ts = [:skipmissing => true, :view => true]

    df2 = @rsubset(df, :a == 1; :view => true)
    @test df2 == correct

    df2 = @rsubset(df, :a == 1; :view => true, :skipmissing => false)
    @test df2 == correct

    df2 = @rsubset(df, :a == 1; t...)
    @test df2 == correct

    df2 = @rsubset(df, :a == 1; ts...)
    @test df2 == correct

    df2 = @rsubset df begin
        :a == 1
        @kwarg :view => true
    end
    @test df2 == correct

    df2 = @rsubset df begin
        :a == 1
        @kwarg [:view => true]...
    end
    @test df2 == correct

    df2 = @rsubset df begin
        :a == 1
        @kwarg [:view => true, :skipmissing => false]...
    end
    @test df2 == correct

    df2 = @rsubset df begin
        :a == 1
        @kwarg t...
    end
    @test df2 == correct

    df2 = @rsubset df begin
        :a == 1
        @kwarg ts...
    end
    @test df2 == correct
end

@testset "Multiple arguments #399" begin
    correct = df[df.a .== 1, :]
    correct_view = view(df, df.a .== 1, :)

    df2 = @subset(df, :a .== 1, :b .== 3; view = true)
    @test df2 ≈ correct_view

    @test_throws ArgumentError @subset(df, :a .== 1, :b .== 3; skipmissing = false)
    @test_throws ArgumentError @subset(df, :a .== 1, :b .== 3; skipmissing = false, view = true)

    correct = transform(df, :a => ByRow(t -> t + 1) => :y, :b => ByRow(t -> t + 2) => :z)
    df2 = @rtransform(df, :y = :a + 1, :z = :b + 2; copycols = false)
    @test df2 ≅ correct
    @test df.a === df2.a

    correct = DataFrame(b_mean = [3.5, 5.0], b_first = [3, 5])
    df2 = @combine(gd, :b_mean = mean(skipmissing(:b)), :b_first = first(:b); keepkeys = false)
    @test df2 ≅ correct
end

@testset "@kwarg errors" begin

end

end # module