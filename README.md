# Granulate's shared Python utilities


## Testing

```shell
pytest tests
```

## Exposing Exceptions
Since exceptions are unchecked in python, as a general rule 3rd party exceptions should not be "part of the API".
The reasoning is that if we do something like vendor the dependancy or change to a different library, we will break all code that caught such exceptions.
Moreover - the break will be "silent" and cannot be caught by any linting or typing tools.
So for every 3rd party exception that someone might wish to catch - the exception type should be exposed through granulate-utils, either by catching the 3rd party exception and re-raising our own exception or by re-exporing the 3rd pary exception in the relevant module.

There are two exceptions for this rule for libraries that are in very common use and in the author's opinon should be added to the standard library:
1. `requests`
2. `psutil`
We treat those libraries as "standard library" in this regard.
