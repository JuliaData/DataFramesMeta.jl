cos(x) = x

function make_vec_to_fun(kw::Expr)

    if kw.head == :(=) || kw.head == :kw
        output = kw.args[1]
        
        membernames = Dict{Any, Symbol}()
        funname = gensym()
        body = DataFramesMeta.replace_syms!(kw.args[2], membernames)
        @show typeof(kw.args[1])
        # inputs = broadcast(s -> s.args[1], keys(membernames))
        if kw.args[1] isa Symbol
            t = quote
                $(Expr(:vect, keys(membernames)...)) => function $funname($(values(membernames)...))
                    $body 
                end => $(QuoteNode(output))
            end
        elseif DataFramesMeta.onearg(kw.args[1], :cols)
            t = quote
                $(Expr(:vect, keys(membernames)...)) => function $funname($(values(membernames)...))
                    $body 
                end => $(output)
            end   
        end

        return t
    else
        println("hit this branch")
        return kw
    end
end

function make_vec_to_fun(kw::QuoteNode)
    return kw
end

function transform_helper2(x, args...)

    t = [make_vec_to_fun(arg) for arg in args]

    quote 
        $DataFrames.transform($x, $(t...))
    end
end

macro transform2(x, args...)
    esc(transform_helper2(x, args...))
end

function based_on_helper2(x, args...)

    t = [make_vec_to_fun(arg) for arg in args]

    quote 
        $DataFrames.combine($x, $(t...))
    end
end

macro based_on2(x, args...)
    esc(based_on_helper2(x, args...))
end

function select_helper2(x, args...)
    t = [make_vec_to_fun(arg) for arg in args]

    quote 
        $DataFrames.select($x, $(t...))
    end
end

macro select2(x, args...)
    esc(select_helper2(x, args...))
end

# df = DataFrame(rand(2,2))
# t = :x2
# s = :y
# @transform2(df, y = :x1 .+ :x2)
# @transform2(df, y = :x1 .+ cols(t))
# @transform2(df, [:x1, :x2])
# @transforms(df, [:x1, :x2], y = :x1 .+ :x2)