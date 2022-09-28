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
df2 = @subset(df, :a .== 1; view = true)
@test df2 ≅ view(df, df.a .== 1, :)
@test_throws ArgumentError @subset(df, :b .== 3; skipmissing = false)
gd2 = @subset(gd, [true, true]; ungroup = false)
@test gd2 ≅ groupby(df, :a)

# @rsubset
# skipmissing, view, ungroup
df2 = @rsubset(df, :a == 1; view = true)
@test df2 ≅ view(df, df.a .== 1, :)
@test_throws ArgumentError @rsubset(df, :b == 3; skipmissing = false)
gd2 = @rsubset(gd, first(true); ungroup = false)
@test gd2 ≅ groupby(df, :a)

# @subset!
# skipmissing, ungroup
@test_throws ArgumentError @subset!(copy(df), :b .== 3; skipmissing = false)
gd2 = @subset!(deepcopy(gd), [true, true]; ungroup = false)
@test gd2 ≅ groupby(df, :a)

# @rsubset!
# skipmissing, ungroup
@test_throws ArgumentError @rsubset!(copy(df), :b == 3; skipmissing = false)
gd2 = @rsubset!(deepcopy(gd), first(true); ungroup = false)
@test gd2 ≅ groupby(df, :a)

# @orderby # Not added
@test_throws MethodError @orderby(df, :a; view = true)

# @rorderby # Not added
@test_throws MethodError @rorderby(df, :a; view = true)

# @select
# copycols, renamecols (not relevant)
# keepkeys, ungroup
df2 = @select(df, :a; copycols = false)
@test df2.a === df.a

df2 = @select(gd, :b; keepkeys = true)
@test df2 ≅ df

gd2 = @select(gd, :b; ungroup = false)
@test gd2 ≅ gd

# @rselect
# copycols, renamecols (not relevant)
# keepkeys, ungroup
df2 = @rselect(df, :a; copycols = false)
@test df2.a === df.a

df2 = @rselect(gd, :b; keepkeys = true)
@test df2 ≅ df

gd2 = @rselect(gd, :b; ungroup = false)
@test gd2 ≅ gd

# @select!
# renamecols (not relevant), ungroup
gd2 = @rselect!(deepcopy(gd), :b; ungroup = false)
@test gd2 ≅ gd

# @rselect!
# renamecols (not relevant), ungroup
gd2 = @rselect!(deepcopy(gd), :b; ungroup = false)
@test gd2 ≅ gd

# @transform
# copycols, renamecols (not relevant)
# ungroup
df2 = @transform(df, :a; copycols = false)
@test df2.a === df.a

gd2 = @transform(gd, :b; ungroup = false)
@test gd2 ≅ gd

# @rtransform
# copycols, renamecols (not relevant)
# ungroup
df2 = @transform(df, :a; copycols = false)
@test df2.a === df.a

gd2 = @transform(gd, :b; ungroup = false)
@test gd2 ≅ gd

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