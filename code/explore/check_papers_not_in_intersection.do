/* ----------------------------------------------------------------------------
	political econ of research
	
	code author: roberto gonzalez
	date: january 26, 2026
	
	objective: get list of papers in OpenAlex but not in NBER or viceversa
	and manually check if they show up
---------------------------------------------------------------------------- */

/* define locals to open alex files */
local oa_path "${build}/scrape_policies_openalex/output"

/* define local to nber files */
local nber_path "${build}/scrape_policies_nber/output"

/* define list of policy abbreviations to loop over */
local files : dir "`oa_path'" files "*"

local policy_abb ""
foreach fff of local files {
	local underscore = strpos("`fff'", "_")
	local pol_abb = upper(substr("`fff'", 1, `underscore'-1))
	local policy_abb "`policy_abb' `pol_abb'"
}

local policy_abb : list uniq policy_abb
local drop_all "ALL"
local policy_abb : list policy_abb - drop_all

display in yellow "`policy_abb'"

/* loop over policies and merge the datasets */
foreach ppp in `policy_abb' {
	/* import file with papers */
	import parquet using "`oa_path'/`ppp'_papers_openalex.parquet", clear
	/* keep doi, paper title and authors */
	keep doi title authors source_name
	/* clean name */
	replace title = lower(title)
	/* keep the ones with doi */
	keep if !missing(doi)
	/* save tempfile with these papers */
	tempfile `ppp'_oa
	save ``ppp'_oa'
}

use "`ACA_oa'", clear
local n_init = _N
di in red `n_init'
foreach ppp in `policy_abb' {
	append using "``ppp'_oa'"
}
duplicates drop doi, force
//assert r(N_drop) == `n_init'

/* keep papers in qje and aer */
keep if strpos(lower(source_name), "quarterly journal of economics") | strpos(lower(source_name), "american economic review")