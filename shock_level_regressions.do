cd "C:\Users\yaoan\OneDrive\Documents\PhD Research\Mechanization"
use "regression\output\shock_level_data.dta", clear //data constructed using the ssagregate.rmd in the regression folder


eststo: qui ivreg2 nonag_jpt_share_L1 ( purchase_jpt_share_L1  = subsidy_fd_demean ) [w = share_L1], robust cluster(prv_size)
eststo: qui ivreg2 manufacuturing_jpt_share_L1 ( purchase_jpt_share_L1  = subsidy_fd_demean ) [w = share_L1], robust cluster(prv_size)
eststo: qui ivreg2 construction_jpt_share_L1 ( purchase_jpt_share_L1  = subsidy_fd_demean ) [w = share_L1], robust cluster(prv_size)
eststo: qui ivreg2 wholesaleretail_jpt_share_L1 ( purchase_jpt_share_L1  = subsidy_fd_demean ) [w = share_L1], robust cluster(prv_size)
eststo: qui ivreg2 accommodationfood_jpt_share_L1 ( purchase_jpt_share_L1  = subsidy_fd_demean ) [w = share_L1], robust cluster(prv_size)
eststo: qui ivreg2 logistic_jpt_share_L1 ( purchase_jpt_share_L1  = subsidy_fd_demean ) [w = share_L1], robust cluster(prv_size)

esttab est1 est2 est3 est4 est5 est6 using "regression\output\shock_level_2sls.tex" , ///
				se(3) b(a3) ///
				scalars("widstat  First stage F statistic" "N # observations") ///
                mtitles("All non-ag sectors" "Manufacturing" "Construction"  "Wholesale and retail" "Hotel and food service" "Logistic" ) ///
				label star(* 0.10 ** 0.05 *** 0.01) replace booktabs ///
				title("Shock-level 2SLS results") ///
				width(\hsize) ///
				alignment(D{.}{.}{-1})

eststo: qui aaniv nonag_jpt_share_L1 ( purchase_jpt_share_L1  = subsidy_fd_demean ) [w = share_L1]
eststo: qui aaniv manufacuturing_jpt_share_L1 ( purchase_jpt_share_L1  = subsidy_fd_demean ) [w = share_L1]
eststo: qui aaniv construction_jpt_share_L1 ( purchase_jpt_share_L1  = subsidy_fd_demean ) [w = share_L1]
eststo: qui aaniv wholesaleretail_jpt_share_L1 ( purchase_jpt_share_L1  = subsidy_fd_demean ) [w = share_L1]
eststo: qui aaniv accommodationfood_jpt_share_L1 ( purchase_jpt_share_L1  = subsidy_fd_demean ) [w = share_L1]
eststo: qui aaniv logistic_jpt_share_L1 ( purchase_jpt_share_L1  = subsidy_fd_demean ) [w = share_L1]

esttab est7 est8 est9 est10 est11 est12 using "regression\output\shock_level_u.tex" , ///
				se(3) b(a3) ///
				scalars("N # observations") ///
                mtitles("All non-ag sectors" "Manufacturing" "Construction"  "Wholesale and retail" "Hotel and food service" "Logistic" ) ///
				label star(* 0.10 ** 0.05 *** 0.01) replace booktabs ///
				title("Shock-level Andrews and Armstrong Unbiased Estimator") ///
				width(\hsize) ///
				alignment(D{.}{.}{-1})
