(package-initialize)

;; Add Docker path for oer-reveal, load it
(add-to-list 'load-path "/root/.emacs.d/elpa/emacs-reveal/oer-reveal")
(require 'oer-reveal-publish)

(let ((oer-reveal-submodules-dir "/root/.emacs.d/elpa/emacs-reveal/emacs-reveal-submodules/")
      (oer-reveal-plugins '("reveal.js-jump-plugin"))
      (oer-reveal-plugin-4-config "")
      (oer-reveal-navigation-mode nil)
      (org-re-reveal-revealjs-version "4")
      (org-re-reveal-history t)
      (org-re-reveal--href-fragment-prefix org-re-reveal--slide-id-prefix)
      (org-publish-project-alist
       (list
        (list "assets"
	            :base-directory "assets"
              :include ".*"
	            :publishing-directory "./public/assets"
	            :publishing-function 'org-publish-attachment
	            :recursive t)
        (list "css"
	            :base-directory "css"
              :include ".*"
	            :publishing-directory "./public/css"
	            :publishing-function 'org-publish-attachment
	            :recursive t)
        (list "vendor"
	            :base-directory "vendor"
              :include ".*"
	            :publishing-directory "./public/vendor"
	            :publishing-function 'org-publish-attachment
	            :recursive t)
	      (list "org-presentations"
	            :base-directory "."
	            :include '("slides.org")
              :publishing-function 'org-re-reveal-publish-to-reveal-client
	            :publishing-directory "./public")
        )))
  )


(oer-reveal-publish-all)
