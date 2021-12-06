#!/bin/bash

COMMAND="emacs --batch -L /root/.emacs.d/elpa/emacs-reveal/org-mode/lisp --load /root/.emacs.d/elpa/emacs-reveal/org-re-reveal/org-re-reveal.el --load /src/elisp/publish.el"

docker run --rm -it -v $(pwd):/src -w /src --entrypoint /bin/bash registry.gitlab.com/oer/emacs-reveal/emacs-reveal:8.29.0 -c "$COMMAND"
