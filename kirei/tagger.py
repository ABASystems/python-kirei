import re


class Tagger(object):
    tag_exprs = {}

    def __init__(self):
        self._progs = {}
        for tag, exprs in self.tag_exprs.items():
            if not isinstance(exprs, (list, tuple)):
                exprs = [exprs]
            all_expr = '(?<![a-z])(?:' + ')|(?:'.join(exprs) + ')(?![a-z])'
            self._progs[tag] = re.compile(all_expr, re.I)

    def tag_text(self, text):
        tags = set()
        for tag, prog in self._progs.items():
            if prog.search(text):
                tags.add(tag)
        return tags

    @property
    def tags(self):
        return self.tag_exprs.keys()
