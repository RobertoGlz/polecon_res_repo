/* ----------------------------------------------------------------------------
	political econ of research
									
	code author: Roberto Gonzalez-Tellez
	date: january 26, 2026
	
	Code description: This do file sets up the configurations for relative
	paths, and if desired, runs the do files needed to replicate the 
	progress done.
---------------------------------------------------------------------------- */

/* Boilerplate for a fresh Stata session */
set more off
set varabbrev off
clear all
macro drop _all

/* Set Stata version */
version 19

/* Set up relative paths based on the user --------------------------------- */
/* 
	To replicate in your computer, uncomment the following line by removing 
	the // and change the path to the local directory where you stored the 
	replication folder
*/

// global main "your/local/path/to/replication/folder"

/* Paths for collaborators ------------------------------------------------- */
if "${main}" == "" {
	/* Roberto's Stanford RF Dell laptop */
	if "`c(username)'" == "rob98" {
		global main "C:/Users/rob98/Dropbox/PolEcon_Res"
	}
	/* Wes' desktop */
	else if "`c(username)'" == "woojin" {
		global main "~/Dropbox/"
	}
	else {
		display as error "User is not recognized."
		display as error "Specify the main directory in the make_file_polecon_res.do file"
		exit 198
	}
}

/* Set up configurations by calling config.do */
do "${main}/polecon_res_repo/code/config.do"

/* Exit do file ; if you want to replicate all the project comment out the exit command */
exit

/* Replicate analysis */
/* BUILD ------------------------------------------------------------------- */

/* ANALYSIS ---------------------------------------------------------------- */
