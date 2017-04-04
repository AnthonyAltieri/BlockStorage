# Returns the keys to a map that have a value other than None
def keys(map):
    validKeys = []
    for key in map.keys():
        if map.get(key) != None:
            validKeys.append(key)
    return validKeys

# Prints dictionaries in a neat manner
def pretty(d, indent=0):
    if d == None:
        return
    for key, value in d.iteritems():
        print '\t' * indent + str(key) + ":"
        if isinstance(value, dict):
            pretty(value, indent+1)
        else:
            print '\t' * (indent+1) + str(value)
