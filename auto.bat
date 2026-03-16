@echo off
:: Verifica se o script está sendo executado como administrador
net session >nul 2>&1
if %errorLevel% neq 0 (
    echo Por favor, execute este script como administrador.
    pause
    exit
)

:: Navega até o diretório desejado e ativa o ambiente virtual
cd C:\Users\Administrador\Projetos\infraspeak 3.0\infraspeak
cd .\venv\Scripts
call activate

:: Retorna ao diretório raiz e executa o script Python
cd ..\..\
py -m etls.infraspeak.sync

:: Pausa para ver a saída ou erros
echo.
exit