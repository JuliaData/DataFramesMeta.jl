# DataFramesMeta.jl

Experiments with metaprogramming tools for DataFrames (and maybe other
Julia objects that hold variables). Goals are to improve performance
and provide more convenient syntax.

In earlier versions of DataFrames, expressions were used in indexing
and in `with` and `within` functions. This approach had several
deficiencies. Performance was poor. The functions relied on `eval`
which caused several issues, most notably that results were different
when used in the REPL than when used inside a function.

# Features

## `@with`

```julia
using DataArrays, DataFrames
using DataFramesMeta

df = DataFrame(x = 1:3, y = [2, 1, 2])
x = [2, 1, 0]

@with(df, :y + 1)
@with(df, :x + x)  # the two x's are different

x = @with df begin
    res = 0.0
    for i in 1:length(:x)
        res += :x[i] * :y[i]
    end
    res
end

@with(df, df[:x .> 1, ^(:y)]) # The ^ means leave the :y alone

```

This works for Associative types, too:

```julia
y = 3
d = {:s => 3, :y => 44, :d => 5}

@with(d, :s + :y + y)
```

## `@select`

This is an alternative to `getindex`.

```julia
@select(df, :x .> 1)
@select(df, :x .> x) # again, the x's are different
@select(df, :A .> 1, [:B, :A])
```

# Discussions

Everything here is experimental.

`@with` works by parsing the expression body for all columns
indicated by symbols (e.g. `:colA`). Then, a function is created that
wraps the body and passes the columns as function arguments. This
function is then called. Because a new function is defined, the body
of `@with` can be evaluated effiently. `@select` is based on `@with`.

Right now, `@with` works for both AbstractDataFrames and Associative
types. `@select` really only works for AbstractDataFrames. Because
macros are not type specific, it would be nice to make these
metaprogramming tools as general as possible.

Instead of `:colA` to refer to a member of the type, another option is
to use `*colA` or `^colA` or something else that isn't defined in
Julia (but can be parsed). Then, it'd be easier to mix use of symbols
with column references. `:colA` is most consistent in that it has (I
think) the tightest precedence, so you don't have to worry about using
parentheses. 

From the user's point of view, it'd be nice to swap the
"dereferencing", so in `@with(df, colA + :outsideVariable)`, `colA` is
a column, and `:outsideVariable` is an external variable. That is quite
difficult to do, though. You have to parse the expression tree and
replace all quoted variables with the "right thing". Here's an example
showing some of the difficulties:

```julia
@with df begin
    y = 1 + x + :z  # z is supposed to be an outside variable; x is a column
    fun(x) = x + 1  # don't want to substitute this x
    fun(y + x)      # don't want to substitute this y
end
```

For performance, we should check to see if this can play nicely with
`@devectorize` for use on columns.

