module TestDicts

using Test
using DataFramesMeta

y = 3
d = Dict(:s => 3, :y => 44, :d => 5, :e => :(a + b))
@test @with(d, :s + :y) == d[:s] + d[:y]
@test @with(d, :s + y)  == d[:s] + y
@test @with(d, d)  == d
@test @with(d, :s + d[^(:y)])  == d[:s] + d[:y]
@test @with(d, :e.head) == d[:e].head
@test @with(Dict(:s => 3, :y => 44, :d => 5, :e => :(a + b)), :e.head) == d[:e].head

x = @with d begin
    z = y + :y - 1
    :s + z
end
@test x == y + d[:y] - 1 + d[:s]

fun = d -> @with d begin
    z = y + :y - 1
    :s + z
end
@test fun(d) == y + d[:y] - 1 + d[:s]

d2 = @transform(d, z = :y + :s)
@test d2[:z] == 47

d2 = @select(d, :y, z = :y + :s, :e)
@test d2[:z] == 47

@test DataFramesMeta.with_helper(:df, :(f.(1))) == :(f.(1))
@test DataFramesMeta.with_helper(:df, :(f.(b + c))) == :(f.(b + c))
@test DataFramesMeta.with_helper(:df, Expr(:., :a, QuoteNode(:b))) == Expr(:., :a, QuoteNode(:b))
@test DataFramesMeta.select_helper(:df, 1).args[2].args[2].args[2].args[3] == 1
@test DataFramesMeta.select_helper(:df, QuoteNode(:a)).args[2].args[2].args[2].args[2].args[2].args[2].args[3].args[1] == :a
@test DataFramesMeta.expandargs(QuoteNode(:a)) == Expr(:kw, :a, QuoteNode(:a))
@test DataFramesMeta.byrow_find_newcols(:(;), Any[]) == (Any[], Any[])

end # module
