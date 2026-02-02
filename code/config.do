/* ----------------------------------------------------------------------------
	political economy of research project
									
	code author: Roberto Gonzalez-Tellez
	date: january 26, 2026
	
	Code description: This do file sets up the configurations for all relative
	paths, significance stars for tables and colors to be used in graphs
---------------------------------------------------------------------------- */

/* Set up global to subfolders in ${main}/polecon_res_repo/code */
foreach subfolder in "analysis" "build" "explore" {
	global `subfolder' "${main}/polecon_res_repo/code/`subfolder'"
	capture mkdir "${`subfolder'}"
}

/* Set up global to source data folder */
global src "${main}/polecon_res_src"

/* Set up global to working folder */
global work "${main}/polecon_res_work"
capture mkdir "${work}"

/* set uip global to results folder */
global results "${main}/polecon_res_repo/results"
capture mkdir "${results}"

/* Set up significance stars */
global star "star(* 0.1 ** 0.05 *** 0.01)"

/* Set up colors */
global br_blue "15 82 186"
global br_red "220 0 0"

/* Let the user know configurations were successfully set */
display in yellow "Configurations successfully set (:"
