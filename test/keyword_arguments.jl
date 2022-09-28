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
    correcty = view(df, df.a .== 1, :)
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
    correct = df.a

    df2 = @transform(df, :a; copycols = false)
    @test df2.a === correct

    df2 = @transform df begin
        :a
        @kwarg copycols = false
    end
    @test df2.a === correct

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
    correct = df.a

    df2 = @rtransform(df, :a; copycols = false)
    @test df2.a === correct

    df2 = @rtransform df begin
        :a
        @kwarg copycols = false
    end
    @test df2.a === correct

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
gd2 = @transform!(deepcopy(gd), :b; ungroup = false)
@test gd2 ≅ gd

# @rtransform!
# renamecols (not relevant), ungroup
gd2 = @transform!(deepcopy(gd), :b; ungroup = false)
@test gd2 ≅ gd

# @combine
# renamecols (not relevant), keepkeys,
# ungroup
df2 = @combine(gd, :b_f = first(:b); keepkeys = true)

@test sort(df2, :a) ≅ DataFrame(a = [1, 2], b_f = [3, 5])

gd2 = @combine(gd, :b = :b; ungroup = false)
@test gd2 ≅ gd

# @by
# renamecols (not relevant), keepkeys,
# ungroup
df2 = @by(df, :a, :b_f = first(:b); keepkeys = true)

@test sort(df2, :a) ≅ DataFrame(a = [1, 2], b_f = [3, 5])

gd2 = @by(df, :a, :b = :b; ungroup = false)
@test gd2 ≅ gd

end # module