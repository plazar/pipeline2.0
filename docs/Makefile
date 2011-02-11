all: documentation.pdf

documentation.pdf: documentation.tex
	pdflatex documentation.tex
	pdflatex documentation.tex

.PHONY: clean
clean: 
	rm -f *.{aux,bbl,blg,log,lot,lof,toc,fot}

.PHONY: cleaner
cleaner: clean
	rm -f *.pdf
