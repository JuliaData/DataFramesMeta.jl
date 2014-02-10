module TestChaining

using Base.Test
using DataArrays, DataFrames
using DataFramesMeta

##############################################################################
##
## @as macro for expression chaining
## from James Porter
## https://github.com/JuliaLang/julia/pull/5734
##
##############################################################################

function asexpand(expr)
    if isa(expr,Expr) && expr.head == :call && expr.args[1] == :|>
        return [asexpand(expr.args[2]) expr.args[3]]
    else
        return expr
    end
end

macro as(name, bindings)
    if !isa(bindings, Expr)
        error("malformed @as bindings")
    end

    if bindings.head == :block
        exprs = filter(x-> !isa(x,Expr) || x.head != :line, bindings.args)
    elseif bindings.head == :call
        exprs = asexpand(bindings)
    end

    quote
        let $([Expr(:(=),name,expr) for expr in exprs]...)
            $name
        end
    end
end

##############################################################################
##
## @> macro for expression chaining
## adapted from @> by Mike Innes in his Lazy.jl package
## https://github.com/one-more-minute/Lazy.jlJ
##
##############################################################################

thread_left(x) = thread_left(filter(x-> !isa(x,Expr) || x.head != :line, x.args)...)

function thread_left(x, expr, exprs...)
  if typeof(expr) == Symbol
    call = Expr(:call, expr, x)

  elseif typeof(expr) == Expr && expr.head in [:call, :macrocall]
    call = Expr(expr.head, expr.args[1], x, expr.args[2:]...)

  elseif typeof(expr) == Expr && expr.head == :->
    call = Expr(:call, expr, x)

  else
    error("Unsupported expression $expr in @>")
  end
  isempty(exprs) ? call : :(@> $call $(exprs...))
end

macro >(exprs...)
    esc(thread_left(exprs...))
end


##############################################################################
##
## The test...
##
##############################################################################

srand(1)
n = 100
df = DataFrame(a = rand(1:3, n),
               b = ["a","b","c","d"][rand(1:4, n)],
               x = rand(n))

x = @sub(df, :a .> 2)
x = @transform(x, y = 10 * :x)
x = @by(x, :b, meanX = mean(:x), meanY = mean(:y))
x = orderby(x, :meanX)
x = select(x, [:meanX, :meanY, :b])

x_as = @as _ begin
    df
    @sub(_, :a .> 2)
    @transform(_, y = 10 * :x)
    @by(_, :b, meanX = mean(:x), meanY = mean(:y))
    orderby(_, :meanX)
    select(_, [:meanX, :meanY, :b])
end

x_thread = @> begin
    df
    @transform(y = 10 * :x)
    @sub(:a .> 2)
    @by(:b, meanX = mean(:x), meanY = mean(:y))
    orderby(:meanX)
    select([:meanX, :meanY, :b])
end

@test x == x_as
@test x == x_thread

# Basic Julia chaining:

x = df |> @sub(:a .> 2) |> orderby(:x) |> select([:b, :x])
y = sort(df[df[:a] .> 2, [:b, :x]], cols = :x)
@test  x == y

end # module
