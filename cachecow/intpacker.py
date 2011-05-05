'''
Functions for encoding and decoding ints as a string of ASCII characters.
Optimizes for short strings.
'''
import string

_ALPHABET = (string.ascii_uppercase
             + string.ascii_lowercase
             + string.digits
             + '_=') # Just use a few extra chars that won't look too weird.
_ALPHABET_REVERSE = dict((c, i) for (i, c) in enumerate(_ALPHABET))
_BASE = len(_ALPHABET)

def pack_int(n):
    s = []
    while True:
        n, r = divmod(n, _BASE)
        s.append(_ALPHABET[r])
        if n == 0:
            break
        elif n == -1:
            s.append('-')
            break
    return ''.join(reversed(s))

def unpack_int(s):
    n = 0
    for c in s:
        n = n * _BASE + _ALPHABET_REVERSE[c]
    return n


