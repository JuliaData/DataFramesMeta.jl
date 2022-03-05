# DataFramesMeta.jl Tutorial

This is a port of the HarvardX series PH525x Genomics class tutorial on dplyr. View the original [here](https://genomicsclass.github.io/book/pages/dplyr_tutorial.html) and the source [here](https://github.com/genomicsclass/labs/blob/master/intro/dplyr_tutorial.Rmd).

## What is DataFramesMeta.jl?

DataFramesMeta.jl is a Julia package to transform and summarize tabular data. It uses Julia macros to create a domain-specific language for convenient syntax to work with data frames from [DataFrames.jl](https://github.com/JuliaData/DataFrames.jl). DataFramesMeta.jl mirrors concepts in DataFrames.jl as closely as possible, without implementing new features on it's own. For a deeper explanation of DataFramesMeta.jl, see the [documentation](https://github.com/JuliaData/DataFramesMeta.jl). 

DataFramesMeta.jl is heavily inspired by R's [`dplyr`](https://cran.r-project.org/web/packages/dplyr/vignettes/dplyr.html). If you are familiar with `dplyr` this guide should get you up to speed with DataFramesMeta.jl. 

However this tutorial assumes a cursory knowledge of DataFrames.jl. For instance, you should be familiar with the concept of a symbol in Julia (`:x`), and how it is used to index a data frame in DataFrames.jl, such as with `df[:, :x]`. 

## Why Is It Useful?

Like dplyr, the DataFramesMeta.jl package contains a set of macros (or "verbs") that perform common data manipulation operations such as filtering for rows, selecting specific columns, re-ordering rows, and adding new columns. 

In addition, DataFramesMeta.jl makes it easier to summarize data with the `@combine` verb, which implements the [split-apply-combine](https://dataframes.juliadata.org/stable/man/split_apply_combine/) pattern commonly seen in `dplyr` and other data manipulation packages.

## How Does It Compare To Using Base Functions in Julia and in DataFrames.jl?

If you are familiar with Julia, you are probably familiar with base Julia functions such  `map`, and `broadcast` (akin to `*apply` in R). These functions are convenient to use, but are designed to work with arrays, not tabular data.

DataFrames.jl provides the functions `select`, `transform`, and more to work with data frames. Unlike `map` and `broadcast`, these functions are designed to work with tabular data, but have a complicated syntax. 

DataFramesMeta.jl provides a convenient syntax for working with the vectors in a data frame so that you get all the convenience of Base Julia and DataFrames combined. 

## How Do I Get DataFramesMeta.jl? 

To install DataFramesMeta.jl, which also installs DataFrames.jl:

```julia
import Pkg
Pkg.activate(; temp=true) # activate a temprary environment for this tutorial
Pkg.add("DataFramesMeta");
```

To load DataFramesMeta.jl, which also loads DataFrames.jl:

```@repl 1
using DataFramesMeta
```

For this tutorial, we will install some additional packages as well. 

```julia
Pkg.add(["CSV", "HTTP"])
```

Now we load them. We also load the Statistics standard library, which is shipped with Julia, so does not need to be installed.

```@repl 1
using CSV, HTTP, Statistics
```

We will use [CSV.jl](https://csv.juliadata.org/stable/) and [HTTP.jl](https://juliaweb.github.io/HTTP.jl/stable/) for downloading our dataset from the internet.

# Data: Mammals Sleep

The `msleep` (mammals sleep) data set contains the sleep times and weights for a set of mammals and is available in the dagdata repository on GitHub. This data set contains 83 rows and 11 variables.  

We can load the data directly into a DataFrame from the `url`. 

```@repl 1
url = "https://raw.githubusercontent.com/genomicsclass/dagdata/master/inst/extdata/msleep_ggplot2.csv"
msleep = CSV.read(HTTP.get(url).body, DataFrame; missingstring="NA")
```

The columns (in order) correspond to the following: 

column name | Description
--- | ---
`:name` | common name
`:genus` | taxonomic rank
`:vore` | carnivore, omnivore or herbivore?
`:order` | taxonomic rank
`:conservation` | the conservation status of the mammal
`:sleep_total` | total amount of sleep, in hours
`:sleep_rem` | rem sleep, in hours
`:sleep_cycle` | length of sleep cycle, in hours
`:awake` | amount of time spent awake, in hours
`:brainwt` | brain weight in kilograms
`:bodywt` | body weight in kilograms


# Important DataFramesMeta.jl Verbs To Remember

Many DataFrames.jl macros come in two forms, a version which operates on columns as a whole and a version which operations row-wise, prefixed by `r`.

DataFramesMeta.jl macro | By-row version | Description | `dplyr` equivalent
--- | --- | --- | ---
`@select` | `@rselect`| select columns | `select`
`@transform` | `@rtransform` | create new columns | `mutate`
`@subset` | `@rsubset` | filter rows | `filter`
`@orderby` | `@rorderby` | re-order or arrange rows | `arrange`
`@combine` | | summarise values | `summarize` (but `@combine` is more flexible)
`groupby` | | allows for group operations in the "split-apply-combine" concept | `group_by`

# DataFramesMeta.jl Verbs In Action

Two of the most basic functions are `@select` and `@subset`, which selects columns and filters rows respectively. To reference columns, use the `Symbol` of the column name, i.e. `:name` refers to the column `msleep.name`.

## Selecting Columns Using `@select`

Select a set of columns: the `:name` and the `:sleep_total` columns. 

```@repl 1
@select msleep :name :sleep_total
```

If you have a column name stored as a variable, you can select it as a column with the syntax `$`. 

```@repl 1
varname = :sleep_total
@select msleep :name $varname
```

The `$` sign has special meaning in DataFramesMeta.jl. We use it for any column reference which is *not* a symbol. Without it, DataFramesMeta.jl can't tell whether `varname` refers to the column `:sleep_total`. 

You can also use `$` to refer to columns with strings

```@repl 1
varname = "sleep_total"
@select msleep :name $varname
```

as well as vectors of variable names 

```@repl 1
varnames = ["name", "sleep_total"]
@select msleep $varnames
```

Similarly, to select the first column, use the syntax `$1`. 
 
```@repl 1
@select msleep $1
```

To select all the columns *except* a specific column, use the `Not` function for inverse selection. We also need to wrap `Not` in the `$` sign, because it is not a symbol. 

```@repl 1
@select msleep $(Not(:name))
```

To select a range of columns by name, use the `Between` operator:

```@repl 1
@select msleep $(Between(:name, :order))
```

To select all columns that start with the character string `"sl"` use [regular expressions](https://regexone.com/):

```@repl 1
@select msleep $(r"^sl")
```

Regular expressions are powerful, but can be difficult for new users to understand. Here are some quick tips.  

1. `r"^abc"` = Starts with `"abc"`
2. `r"abc$"` = Ends with `"abc"`
3. `r"abc"` = Contains `"abc"` anywhere. 

## Selecting Rows Using `@subset` and `@rsubset`

Filter the rows for mammals that sleep a total of more than 16 hours. 

```@repl 1
@subset msleep :sleep_total .>= 16
```

In the above expression, the `.>=` means we "broadcast" the `>=` comparison across the whole column. We can use a simpler syntax, `@rsubset` which automatically broadcasts all operations.

```@repl 1
@rsubset msleep :sleep_total > 16
```

Subset the rows for mammals that sleep a total of more than 16 hours *and* have a body weight of greater than 1 kilogram. For this we put multiple operations on separate lines in a single block. 

```@repl 1
@rsubset msleep begin 
    :sleep_total >= 16 
    :bodywt >= 1
end
```

If you are coming from `dplyr`, you can also write the above command in a way that looks more familiar. 

```@repl 1
@rsubset(msleep, :sleep_total >= 16, :bodywt >= 1)
```

Filter the rows for mammals in the Perissodactyla and Primates taxonomic order. We wrap code in a `let` block to ensure things are fast.

```@repl 1
let
    relevant_orders = Set(["Perissodactyla", "Primates"])
    @rsubset msleep :order in relevant_orders
end
```

You can use the boolean operators (e.g. >, <, >=, <=, !=, in) to create the logical tests. 

# Chain.jl

Before we go any further, let's introduce the `@chain` macro from [Chain.jl](https://github.com/jkrumbiegel/Chain.jl), which is re-exported by DataFramesMeta.jl. `@chain` allows for you to pipe the output from one operation into the input of another operation.  The idea of piping is to read the functions from left to right. The syntax and design of `@chain` is very similar to `%>%` which users of dplyr are familiar with.

To show it's usefulness, let's use `@select` and `@rsubset` one after the other. 

```
msleep_1 = @select msleep :name :sleep_total
msleep_2 = @rsubset msleep_1 :sleep_total > 16
```

Now in this case, we will pipe the msleep data frame to the function that will select two columns (`:name` and `:sleep_total`) and then pipe the new data frame to the `@rsubset` opertaion. This method involves awkwardly creating and naming temporary data frames. We can avoid this with `@chain`.

```@repl 1
@chain msleep begin 
    @select :name :sleep_total
    @rsubset :sleep_total > 16
end
```

You will soon see how useful the `@chain` macro is when we start to combine many functions.  

# Back To dplyr Verbs In Action

Now that you know about the `@chain` macro, we will use it throughout the rest of this tutorial. 

## Arrange Or Re-order Rows Using `@orderby`

To arrange (or re-order) rows by a particular column, such as the taxonomic order, list the name of the column you want to arrange the rows by:

```@repl 1
@orderby msleep :order
```

Now we will select three columns from msleep, arrange the rows by the taxonomic order and then arrange the rows by `:sleep_total`. Finally, keep the first 10 rows of the data frame.

```@repl 1
@chain msleep begin 
    @select :name :order :sleep_total
    @orderby :order :sleep_total
    first(10)
end
```

The last line of the above block, `first(10)`, does not have `@`. This is because `first` is a Julia function, not a macro, whose names always begin with `@`. 

Same as above, except here we filter the rows for mammals that sleep for 16 or more hours, instead of showing the head of the final data frame:

```@repl 1
@chain msleep begin 
    @select :name :order :sleep_total
    @orderby :order :sleep_total 
    @rsubset :sleep_total > 16
end
```

Something slightly more complicated: same as above, except arrange the rows in the `:sleep_total` column in a descending order. For this, use the function `sortperm` with the keyword argument `rev=true`.

```@repl 1
@chain msleep begin 
    @select :name :order :sleep_total
    @orderby begin 
        :order 
        sortperm(:sleep_total, rev=true)
    end 
    @rsubset :sleep_total >= 16
end
```

## Create New Columns Using `@transform` and `@rtransform`

The `@transform` macro will add new columns to the data frame. Like with other macros, use `@rtransform` to operate row-wise. Create a new column called `:rem_proportion`, which is the ratio of rem sleep to total amount of sleep. 

```@repl 1
@rtransform msleep :rem_proportion = :sleep_rem / :sleep_total
```

You can add multiple columns at a time by placing the operations in a block.

```@repl 1
@rtransform msleep begin 
    :rem_proportion = :sleep_rem / :sleep_total 
    :bodywt_grams = :bodywt * 1000
end
```

Using `@transform` instead of `@rtransform` will let us work with the column as a whole, and not a single row at a time. Let's create a new variable showing how far an animal's sleep time is from the average of all animals. 

```@repl 1
@transform msleep :demeand_sleep = :sleep_total .- mean(:sleep_total)
```

Finally, note that you can create a new column with the name taken from an existing variable, or a new column name with spaces in it, with `$`

```@repl 1
newname = :rem_proportion
@rtransform msleep begin 
    $newname = :sleep_rem / :sleep_total
    $"Body weight in grams" = :bodywt * 1000
end
```

## Create Summaries of the Data Frame using `@combine`

The `@combine` macro will create summary statistics for a given column in the data frame, such as finding the mean. For example, to compute the average number of hours of sleep, apply the `mean` function to the column `:sleep_total` and call the summary value `:avg_sleep`. 

```@repl 1
@chain msleep @combine :avg_sleep = mean(:sleep_total)
```

There are many other summary statistics you could consider such `std`, `minimum`, `maximum`, `median`, `sum`, `length` (returns the length of vector), `first` (returns first value in vector), and `last` (returns last value in vector).

```@repl 1
@combine msleep begin 
    :avg_sleep = mean(:sleep_total) 
    :min_sleep = minimum(:sleep_total)
    :max_sleep = maximum(:sleep_total)
    :total = length(:sleep_total)
end
```

DataFrames.jl also provides the function `describe` which performs many of these summaries automatically. 

```@repl 1
describe(msleep)
```

## Group Operations using `groupby` and `@combine`

The `groupby` verb is an important function in DataFrames.jl (it does not live in DataFramesMeta.jl). As we mentioned before it's related to concept of "split-apply-combine". We literally want to split the data frame by some variable (e.g. taxonomic order), apply a function to the individual data frames and then combine the output.   

Let's do that: split the `msleep` data frame by the taxonomic order, then ask for the same summary statistics as above. We expect a set of summary statistics for each taxonomic order. 

```@repl 1
@chain msleep begin 
    groupby(:order)
    @combine begin 
        :avg_sleep = mean(:sleep_total)
        :min_sleep = minimum(:sleep_total)
        :max_sleep = maximum(:sleep_total)
        :total = length(:sleep_total)
    end
end
```

Split-apply-combine can also be used with `@transform` to add new variables to a data frame by performing operations by group. For instance, we can de-mean the total hours of sleep of an animal relative to other animals in the same genus. 

```@repl 1
@chain msleep begin 
    groupby(:order)
    @transform :sleep_genus = :sleep_total .- mean(:sleep_total)
end
```

This short tutorial only touches on the wide array of features in Julia, DataFrames.jl, and DataFramesMeta.jl. Read the [full documentation](https://github.com/JuliaData/DataFramesMeta.jl) for more information.
