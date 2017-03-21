
::C:\Windows\SysWOW64\WindowsPowerShell\v1.0\powershell.exe "C:\Batches\AutomationProjects\Watcher\code\bat\run_ssrs_medianperformancefull.ps1"

@echo off
for /F %%i in ('C:\Windows\SysWOW64\WindowsPowerShell\v1.0\powershell.exe -noprofile -File "C:\Batches\AutomationProjects\Watcher\code\bat\run_ssrs_PerformanceByStrategy.ps1"') do set myfile=%%i
::pause
echo %myfile%
echo %myfile% 2
echo %myfile% 3
echo %myfile% 4
echo %myfile%
