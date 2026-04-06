from reporters.daily import DailyReportGenerator
gen = DailyReportGenerator()
report = gen.generate()
print(report.get("title"))
