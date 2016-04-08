# browser-based-map-reduce

###Example

####WordCount

~~~~{.javascript}
function __map_function(input)
{
  var split = input.split(" "),
    obj = {};

  for (var x=0; x<split.length; x++){
    if(obj[split[x]]===undefined){
       obj[split[x]]=1;
    }
    else{
       obj[split[x]]++;
    }
  }
 return obj;
}
~~~~
~~~~{.python}
def __reduce_function(final_result, taskoutput):
    for i in taskoutput:
        if i in final_result:
            final_result[i] += taskoutput[i]
        else:
            final_result[i] = taskoutput[i]
    return final_result
~~~~

