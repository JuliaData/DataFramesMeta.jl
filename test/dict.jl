module TestDicts

using Base.Test
using DataFramesMeta
y = 3
d = {:s => 3, :y => 44, :d => 5, :e => :(a + b)}
@test @with(d, :s + :y) == d[:s] + d[:y]
@test @with(d, :s + y)  == d[:s] + y
@test @with(d, d)  == d
@test @with(d, :s + d[^(:y)])  == d[:s] + d[:y]
@test @with(d, :e.head) == d[:e].head

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

end # module
