#
# Copyright 2013 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import heapq

from six.moves import reduce


def _decorate_source(source):
    for message in source:
        yield ((message.dt, message.source_id), message)



def date_sorted_sources(*sources):
    """
    Takes an iterable of sources, generating namestrings and
    piping their output into date_sort.
    """
    sorted_stream = heapq.merge(*(_decorate_source(s) for s in sources))



    # Strip out key decoration
    for _, message in sorted_stream:
        yield message
'''

for i in _decorate_source(source):
    print i
(('20160315', 'toby'), <__main__.student instance at 0x00000000174BD348>)
(('20161128', 'claire'), <__main__.student instance at 0x00000000174DF308>)

sorted_stream = heapq.merge(*(_decorate_source(s) for s in [source, source2]))
for item in sorted_stream:
    print item[0]
    
('19912032', 675)
('20160315', 'toby')
('20161128', 'claire')
('34245533', 482)
'''

def sequential_transforms(stream_in, *transforms):
    """
    Apply each transform in transforms sequentially to each event in stream_in.
    Each transform application will add a new entry indexed to the transform's
    hash string.
    """
    # Recursively apply all transforms to the stream.
    stream_out = reduce(lambda stream, tnfm: tnfm.transform(stream),
                        transforms,
                        stream_in)

    return stream_out
'''
f seq = [ s1, s2, s3, ... , sn ], calling reduce(func, seq) works like this:
At first the first two elements of seq will be applied to func,\
 i.e. func(s1,s2) The list on which reduce() works looks now like\
 this: [ func(s1, s2), s3, ... , sn ]
 i.e.
 reduce(lambda x, y: x+y, [1, 2, 3, 4, 5],7)
 22
'''