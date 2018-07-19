from bs4 import BeautifulSoup
import csv

infile = open("RiskParam.spn",'r')
contents = infile.read()
soup = BeautifulSoup(contents,'xml')
dSpread = soup.find_all('dSpread')

# print(dSpread) #'dSpread_Id',

headers = ['Spread','chargeMeth','r','val','CC']

with open("Span_dSpread.csv", 'w') as myfile:
				wr = csv.writer(myfile)			
				wr.writerow(headers)
# cnt = 0
for dsp in dSpread:
	# cnt = 1
	dsp_list = []
	# dsp_list.append(cnt)
	dsp_list.append(dsp.find('spread').get_text())
	dsp_list.append(dsp.find('chargeMeth').get_text())
	dsp_list.append(dsp.find('r').get_text())
	dsp_list.append(dsp.find('val').get_text())
	try:
		dsp_list.append(dsp.find('cc').get_text())
	except:
		print("error")
		continue
	finally:

		with open("Span_dSpread.csv", 'a') as myfile:
				wr = csv.writer(myfile)			
				wr.writerow(dsp_list)	
		# cnt = cnt+1	
			
		
pLeg = soup.find_all('pLeg')
headers2 = ['CC', 'pe', 'rs', 'i']

with open("Span_dSpread_pLeg.csv",'w') as mycsvfile:
	wr = csv.writer(mycsvfile)
	wr.writerow(headers2)

for pl in pLeg:
	pLeg_list = []
	pLeg_list.append(pl.find('cc').get_text())
	pLeg_list.append(pl.find('pe').get_text())
	pLeg_list.append(pl.find('rs').get_text())
	pLeg_list.append(pl.find('i').get_text())

	with open("Span_dSpread_pLeg.csv",'a') as mycsvfile:
		wr = csv.writer(mycsvfile)
		wr.writerow(pLeg_list)


futpf = soup.find_all('futPf')
headers3 = ['pfId','CID','PE','FUT_p','FUT_d','FUT_v','FUT_cvf','FUT_sc','FUT_setlDate','FUT_t','undC_exch','undC_pfId','undC_cId','undC_s','undC_i','intrRate_val','intrRate_rl','intrRate_cpm','intrRate_exm','scanRate_r','scanRate_priceScan','scanRate_volScan','ra_r','A1','A2','A3','A4','A5','A6','A7','A8','A9','A10','A11','A12','A13','A14','A15','A16']

with open("Span_Fut.csv", 'w') as myfile:
			wr = csv.writer(myfile)			
			wr.writerow(headers3)

for fpf in futpf:
	fut_tag = fpf.find_all('fut')
	for ft in fut_tag:
		futpf_list = []
		futpf_list.append(fpf.find('pfId').get_text())
		futpf_list.append(ft.find('cId').get_text())
		futpf_list.append(ft.find('pe').get_text())
		futpf_list.append(ft.find('p').get_text())
		futpf_list.append(ft.find('d').get_text())
		futpf_list.append(ft.find('v').get_text())
		futpf_list.append(ft.find('cvf').get_text())
		futpf_list.append(ft.find('sc').get_text())
		futpf_list.append(ft.find('setlDate').get_text())
		futpf_list.append(ft.find('t').get_text())

		futpf_list.append(ft.find('undC').find('exch').get_text())
		futpf_list.append(ft.find('undC').find('pfId').get_text())
		futpf_list.append(ft.find('undC').find('cId').get_text())
		futpf_list.append(ft.find('undC').find('s').get_text())
		futpf_list.append(ft.find('undC').find('i').get_text())

		futpf_list.append(ft.find('intrRate').find('val').get_text())
		futpf_list.append(ft.find('intrRate').find('rl').get_text())
		futpf_list.append(ft.find('intrRate').find('cpm').get_text())
		futpf_list.append(ft.find('intrRate').find('exm').get_text())

		futpf_list.append(ft.find('scanRate').find('r').get_text())
		futpf_list.append(ft.find('scanRate').find('priceScan').get_text())
		futpf_list.append(ft.find('scanRate').find('volScan').get_text())

		count = 0
		find_ra_tag = ft.find('ra')

		for r_tag in [find_ra_tag]:		
			futpf_list.append(r_tag.find('r').get_text() if r_tag.find('r').get_text() else '0')

			a_tag = r_tag.find_all('a')
			
			for r in range(len(a_tag)):				
				count = count + 1
				if count == 1:		
					futpf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count== 2:		
					futpf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==3:		
					futpf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==4:	
					futpf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==5:		
					futpf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==6:		
					futpf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==7:		
					futpf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==8:		
					futpf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==9:		
					futpf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==10:		
					futpf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==11:		
					futpf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==12:		
					futpf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==13:		
					futpf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==14:		
					futpf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==15:		
					futpf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==16:		
					futpf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				else:		
					count = 0

		with open("Span_Fut.csv", 'a') as myfile:
			wr = csv.writer(myfile)			
			wr.writerow(futpf_list)
	

fut = soup.find_all('futPf')
headers4 = ['pfId','pfCode','name','currency','cvf','priceDl','priceFmt','valueMeth','priceMeth','setlMeth','undpf_exch','undpf_pfId','undpf_pfCode','undpf_pfType','undpf_s','undpf_i']

with open("Span_FutPf.csv", 'w') as myfile1:
	wr = csv.writer(myfile1)			
	wr.writerow(headers4)

for f in fut:
	futpf_list = []

	futpf_list.append(f.find('pfId').get_text())
	futpf_list.append(f.find('pfCode').get_text())
	futpf_list.append(f.find('name').get_text())
	futpf_list.append(f.find('currency').get_text())
	futpf_list.append(f.find('cvf').get_text())
	futpf_list.append(f.find('priceDl').get_text())
	futpf_list.append(f.find('priceFmt').get_text())
	futpf_list.append(f.find('valueMeth').get_text())
	futpf_list.append(f.find('priceMeth').get_text())
	futpf_list.append(f.find('setlMeth').get_text())

	undpf = f.find('undPf')

	futpf_list.append(undpf.find('exch').get_text())
	futpf_list.append(undpf.find('pfId').get_text())
	futpf_list.append(undpf.find('pfCode').get_text())
	futpf_list.append(undpf.find('pfType').get_text())
	futpf_list.append(undpf.find('s').get_text())
	futpf_list.append(undpf.find('i').get_text())

	with open("Span_FutPf.csv", 'a') as myfile1:
		wr = csv.writer(myfile1)			
		wr.writerow(futpf_list)


oopPf = soup.find_all('oopPf')
headers5 = ['pfId', 'pfCode', 'name', 'Exercise', 'currency', 'cvf', 'priceDl', 'priceFmt', 'strikeDl', 'strikeFmt', 'cab', 'valueMeth', 'priceMeth', 'setlMeth', 'priceModel', 'undpf_exch', 'undpf_pfId', 'undpf_pfCode', 'undpf_pfType', 'undpf_s', 'undpf_i']

with open("Span_oopPf.csv", 'w') as myfile2:
	wr = csv.writer(myfile2)			
	wr.writerow(headers5)

for o in oopPf:
	oopPf_list = []

	oopPf_list.append(o.find('pfId').get_text())
	oopPf_list.append(o.find('pfCode').get_text())
	oopPf_list.append(o.find('name').get_text())
	oopPf_list.append(o.find('exercise').get_text())
	oopPf_list.append(o.find('currency').get_text())
	oopPf_list.append(o.find('cvf').get_text())
	oopPf_list.append(o.find('priceDl').get_text())
	oopPf_list.append(o.find('priceFmt').get_text())
	oopPf_list.append(o.find('strikeDl').get_text())
	oopPf_list.append(o.find('strikeFmt').get_text())
	oopPf_list.append(o.find('cab').get_text())
	oopPf_list.append(o.find('valueMeth').get_text())
	oopPf_list.append(o.find('priceMeth').get_text())
	oopPf_list.append(o.find('setlMeth').get_text())
	oopPf_list.append(o.find('priceModel').get_text())

	undpf = o.find('undPf')

	oopPf_list.append(undpf.find('exch').get_text())
	oopPf_list.append(undpf.find('pfId').get_text())
	oopPf_list.append(undpf.find('pfCode').get_text())
	oopPf_list.append(undpf.find('pfType').get_text())
	oopPf_list.append(undpf.find('s').get_text())
	oopPf_list.append(undpf.find('i').get_text())

	with open("Span_oopPf.csv", 'a') as myfile2:
		wr = csv.writer(myfile2)			
		wr.writerow(oopPf_list)

ooppf_opt = soup.find_all('oopPf')
#####'pfId', 'pe', 'setlDate', 'series_Id', 'pq',
headers6 = ['pfId', 'pe', 'setlDate','cId', 'o', 'k', 'p',  'd', 'v', 'r', 'A1', 'A2', 'A3', 'A4', 'A5', 'A6', 'A7', 'A8', 'A9', 'A10', 'A11', 'A12', 'A13', 'A14', 'A15', 'A16']

with open("Span_opt.csv", 'w') as myfile3:
		wr = csv.writer(myfile3)			
		wr.writerow(headers6)

for opf_opt in ooppf_opt:
	opt_tag = opf_opt.find_all('opt')
	for op in opt_tag:
		opt_list = []
		opt_list.append(opf_opt.find('pfId').get_text())
		opt_list.append(opf_opt.find('series').find('pe').get_text())
		opt_list.append(opf_opt.find('series').find('setlDate').get_text())
		opt_list.append(op.find('cId').get_text())
		opt_list.append(op.find('o').get_text())
		opt_list.append(op.find('k').get_text())
		opt_list.append(op.find('p').get_text())
		opt_list.append(op.find('d').get_text())
		opt_list.append(op.find('v').get_text())

		count = 0
		find_ra_tag = op.find('ra')

		for r_tag in [find_ra_tag]:
				
			opt_list.append(r_tag.find('r').get_text() if r_tag.find('r').get_text() else '0')

			a_tag = r_tag.find_all('a')
			
			for r in range(len(a_tag)):				
				count = count + 1
				if count == 1:		
					opt_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count== 2:		
					opt_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==3:		
					opt_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==4:	
					opt_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==5:		
					opt_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==6:		
					opt_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==7:		
					opt_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==8:		
					opt_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==9:		
					opt_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==10:		
					opt_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==11:		
					opt_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==12:		
					opt_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==13:		
					opt_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==14:		
					opt_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==15:		
					opt_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				elif count==16:		
					opt_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

				else:		
					count = 0

		with open("Span_opt.csv", 'a') as myfile3:
			wr = csv.writer(myfile3)			
			wr.writerow(opt_list)
	

pflink = soup.find_all('pfLink')
headers7 = ['Exch', 'Pfid', 'pfCode', 'pfType', 'sc', 'cmbMeth', 'applyBasicRisk']

with open("Span_pfLink.csv", 'w') as myfile4:
		wr = csv.writer(myfile4)			
		wr.writerow(headers7)

for pfl in pflink:
	pfl_list = []

	pfl_list.append(pfl.find('exch').get_text())
	pfl_list.append(pfl.find('pfId').get_text())
	pfl_list.append(pfl.find('pfCode').get_text())
	pfl_list.append(pfl.find('pfType').get_text())
	pfl_list.append(pfl.find('sc').get_text())
	pfl_list.append(pfl.find('cmbMeth').get_text())
	pfl_list.append(pfl.find('applyBasisRisk').get_text())

	with open("Span_pfLink.csv", 'a') as myfile4:
		wr = csv.writer(myfile4)			
		wr.writerow(pfl_list)

	
phypf = soup.find_all('phyPf')
headers8 = ['pfId','pfCode','name','currency','cvf','priceDl','priceFmt','valueMeth','priceMeth','setlMeth','phy_cId','phy_pe','phy_p','phy_d','phy_v','phy_cvf','phy_sc','scanRate_r','priceScan','volScan','ra_r','A1','A2','A3','A4','A5','A6','A7','A8','A9','A10','A11','A12','A13','A14','A15','A16']

with open("Span_PhyPf.csv", 'w') as myfile5:
			wr = csv.writer(myfile5)			
			wr.writerow(headers8)


for p in phypf:
	phypf_list = []

	phypf_list.append(p.find('pfId').get_text())
	phypf_list.append(p.find('pfCode').get_text())
	phypf_list.append(p.find('name').get_text())
	phypf_list.append(p.find('currency').get_text())
	phypf_list.append(p.find('cvf').get_text())
	phypf_list.append(p.find('priceDl').get_text())
	phypf_list.append(p.find('priceFmt').get_text())
	phypf_list.append(p.find('valueMeth').get_text())
	phypf_list.append(p.find('priceMeth').get_text())
	phypf_list.append(p.find('setlMeth').get_text())

	phy = p.find('phy')

	phypf_list.append(phy.find('cId').get_text())
	phypf_list.append(phy.find('pe').get_text())
	phypf_list.append(phy.find('p').get_text())
	phypf_list.append(phy.find('d').get_text())
	phypf_list.append(phy.find('v').get_text())
	phypf_list.append(phy.find('cvf').get_text())
	phypf_list.append(phy.find('sc').get_text())

	scanrate = p.find('scanRate')

	phypf_list.append(scanrate.find('r').get_text())
	phypf_list.append(scanrate.find('priceScan').get_text())
	phypf_list.append(scanrate.find('volScan').get_text())

	count = 0
	find_ra_tag = p.find('ra')

	for r_tag in [find_ra_tag]:
		
		phypf_list.append(r_tag.find('r').get_text() if r_tag.find('r').get_text() else '0')

		a_tag = r_tag.find_all('a')
	
		for r in range(len(a_tag)):				
			count = count + 1
			if count == 1:		
				phypf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

			elif count== 2:		
				phypf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

			elif count==3:		
				phypf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

			elif count==4:	
				phypf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

			elif count==5:		
				phypf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

			elif count==6:		
				phypf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

			elif count==7:		
				phypf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

			elif count==8:		
				phypf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

			elif count==9:		
				phypf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

			elif count==10:		
				phypf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

			elif count==11:		
				phypf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

			elif count==12:		
				phypf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

			elif count==13:		
				phypf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

			elif count==14:		
				phypf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

			elif count==15:		
				phypf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

			elif count==16:		
				phypf_list.append(a_tag[r].get_text() if a_tag[r].get_text() else '0')

			else:		
				count = 0

	
	with open("Span_PhyPf.csv", 'a') as myfile5:
			wr = csv.writer(myfile5)			
			wr.writerow(phypf_list)


ooppf = soup.find_all('oopPf')

headers9 = ['pfId','series_pe','series_v','series_volSrc','series_SetlDate','series_t','series_cvf','series_sc','undC_exch','undC_pfid','undC_cId','undC_s','undC_i','intrRate_val','intrRate_rl','intrRate_cpm','intrRate_exm','scanRate_r','scanRate_priceScan','scanRate_volScan']

with open("Span_series.csv", 'w') as myfile6:
		wr = csv.writer(myfile6)			
		wr.writerow(headers9)


for opf in ooppf:
	series_tag = opf.find_all('series')
	for s in series_tag:
		series_list = []
		series_list.append(opf.find('pfId').get_text())
		series_list.append(s.find('pe').get_text())
		series_list.append(s.find('v').get_text())
		series_list.append(s.find('volSrc').get_text())
		series_list.append(s.find('setlDate').get_text())
		series_list.append(s.find('t').get_text())
		series_list.append(s.find('cvf').get_text())
		series_list.append(s.find('sc').get_text())

		series_list.append(s.find('undC').find('exch').get_text())
		series_list.append(s.find('undC').find('pfId').get_text())
		series_list.append(s.find('undC').find('cId').get_text())
		series_list.append(s.find('undC').find('s').get_text())
		series_list.append(s.find('undC').find('i').get_text())
		

		series_list.append(s.find('intrRate').find('val').get_text())
		series_list.append(s.find('intrRate').find('rl').get_text())
		series_list.append(s.find('intrRate').find('cpm').get_text())
		series_list.append(s.find('intrRate').find('exm').get_text())


		series_list.append(s.find('scanRate').find('r').get_text())
		series_list.append(s.find('scanRate').find('priceScan').get_text())
		series_list.append(s.find('scanRate').find('volScan').get_text())

		with open("Span_series.csv", 'a') as myfile6:
			wr = csv.writer(myfile6)			
			wr.writerow(series_list)



ccdef = soup.find_all('ccDef')
headers10 = ['CC','tn','r','val']

with open("Span_somTiers.csv", 'w') as myfile7:
			wr = csv.writer(myfile7)			
			wr.writerow(headers10)

for cdef in ccdef:
	som_list = []
	som_list.append(cdef.find('cc').get_text())

	somtiers = cdef.find('somTiers') 
	som_list.append(somtiers.find('tn').get_text())
	som_list.append(somtiers.find('r').get_text())
	som_list.append(somtiers.find('val').get_text())

	with open("Span_somTiers.csv", 'a') as myfile7:
			wr = csv.writer(myfile7)			
			wr.writerow(som_list)
