# Buzz SNPS Desktop para Windows

O fork publica uma edição Windows do Buzz Desktop para agentes locais usarem o
relay corporativo, incluindo Git sobre o relay e e-mail dos agentes. A edição é
instalada em paralelo com o Buzz upstream; nenhum dos dois sobrescreve os
binários ou os dados do outro.

## Identidade e comportamento

| Propriedade | Buzz SNPS | Upstream |
|---|---|---|
| Nome do produto/diretório | `Buzz SNPS` | `Buzz` |
| Identificador Tauri | `com.snpsgroup.buzz.desktop` | `xyz.block.buzz.app` |
| Relay padrão | `wss://message-relay.snpsgroup.com` | seleção manual |
| Dados, keyring e deep link | isolados pelo slug `snps` | identidade padrão |
| Instalação NSIS | usuário atual | conforme pacote upstream |
| Auto-update | desativado | canal upstream |

O slug reutiliza o mecanismo de isolamento de builds nomeados do Desktop. Por
isso alguns nomes internos contêm `demo`; eles são estáveis e não identificam um
build descartável. Não reutilizar a identidade upstream: isso reabriria a
sobrescrita durante a instalação padrão.

A primeira versão é **unsigned**. O Windows/SmartScreen pode exigir confirmação
explícita. Assinatura de código e um canal de atualização SNPS ficam fora deste
fluxo até que exista um certificado e uma política de promoção próprios.

## Build na sincronização com upstream

Depois de integrar e validar o upstream na branch `feat/github-bridge`, o push
das mudanças de código executa `SNPS Windows Desktop` no GitHub Actions. Também
é possível executar manualmente por `workflow_dispatch` após um push de
documentação ou para republicar a mesma revisão.

1. Confirmar que a execução pertence ao commit integrado.
2. Baixar o artefato `buzz-snps-windows-<sha>`.
3. Validar `SHA256SUMS.txt` e conferir `BUILD-INFO.json` com o SHA e o relay.
4. Publicar o instalador, o hash e o build info no MinIO em
   `s3://buzz-releases/snps-desktop-<version>/windows-x86_64/`.
5. Baixar os objetos de volta e repetir a validação antes de divulgar.

Exemplo de obtenção do artefato:

```bash
gh run download <run-id> --repo luisgustavooliveira/buzz \
  --name buzz-snps-windows-<sha> --dir /tmp/buzz-snps-windows
cd /tmp/buzz-snps-windows && sha256sum -c SHA256SUMS.txt
```

O MinIO é interno e não recebe upload direto do GitHub Actions. A publicação no
bucket é uma etapa operacional feita a partir do host autenticado, com as
credenciais S3 do OpenBao, sem exibi-las. Preservar as versões anteriores.

## Aceitação em Windows

Antes de distribuir uma nova versão:

1. Instalar em uma máquina com o Buzz upstream já presente.
2. Confirmar que os dois produtos continuam instalados e que o upstream não foi
   substituído.
3. Abrir o Buzz SNPS e confirmar conexão automática com
   `message-relay.snpsgroup.com`.
4. Autenticar a identidade local e validar um clone/push Git autorizado.
5. Executar um agente local, publicar uma resposta e exercitar o envio de e-mail
   permitido pela política do relay.
6. Confirmar que credenciais e dados do perfil upstream não foram modificados.

Rollback: desinstalar apenas `Buzz SNPS` e instalar a versão SNPS anterior. Não
remover o Buzz upstream nem apagar diretórios compartilhados; a edição SNPS usa
estado próprio.
