---
title: Pipeline de Traitement Multimedia
publish: true
---

# Pipeline de Traitement Multimédia & Déploiement Piwigo

Suite outillée pour l'ingestion, le découpage vidéo sans perte, la compression AV1/WebM, l'optimisation multithread WebP, le calcul d'empreintes cryptographiques, la pré-génération CLI des dérivés sous Laragon et la réplication cloud (Mega S4 & o2switch).

---

## 1. Vue d'ensemble du workflow

```
[Médias Bruts] 
       │
       ├──> [Découpage sans perte] ──> [Encodage SVT-AV1 / Opus] ──> [Rclone: Mega S4]
       │
       └──> [Extraction PNG] ──> [Multithread WebP] ──> [Génération Dump SQL MD5]
                                                                  │
                                                                  ▼
[Mise en ligne o2switch] <── [Rclone dérivés] <── [Génération Dérivés CLI] <── [Import DB / Sync Albums]
```

---

## 2. Découpage vidéo sans perte par scène (FFmpeg CLI)

Permet d'extraire des segments temporels ciblés sans réencodage, de manière quasi instantanée et sans altérer la qualité originale.

```bash
ffmpeg -i input.mp4 -ss 00:03:40 -to 00:15:53 -c copy -avoid_negative_ts make_zero output_1.mp4
```

> [!TIP]
> Positionner `-ss` après l'argument d'entrée `-i` préserve la synchronisation précise sur les images clés (*keyframes*), tandis que `-avoid_negative_ts make_zero` réinitialise l'horodatage initial pour éviter tout gel d'image.

---

## 3. Conversion vidéo par lot : SVT-AV1 & WebM

Script PowerShell de conversion vers le format **AV1** / conteneur **WebM** avec tempo thermique.

```powershell
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$outputDir    = "Videos_AV1_WebM"
$preset       = 8
$crf          = 35
$pauseSeconds = 60

$files = Get-ChildItem -Path . -File | Where-Object { $_.Extension -match '^\.(mp4|mkv|mov)$' }
$total = $files.Count

if ($total -eq 0) {
    Write-Host "Aucun fichier vidéo trouvé." -ForegroundColor Yellow
    exit
}

if (!(Test-Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir | Out-Null
}

$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
$currentIndex = 0

foreach ($file in $files) {
    $currentIndex++
    $outputFile = Join-Path $outputDir "$($file.BaseName)_compressed.webm"

    if (Test-Path $outputFile) {
        Write-Host " ⏭️ Ignoré : $($file.Name) existe déjà." -ForegroundColor Gray
        continue
    }

    & ffmpeg -hide_banner -i "$($file.FullName)" `
        -c:v libsvtav1 -preset $preset -crf $crf `
        -c:a libopus -b:a 128k `
        -y "$outputFile"

    if ($currentIndex -lt $total) {
        Start-Sleep -Seconds $pauseSeconds
    }
}

$stopwatch.Stop()
[System.Console]::Beep(880, 500)
```

---

## 4. Optimisation Multithread PNG vers WebP

Conversion haute performance via l'architecture **RunspacePool** avec suppression conditionnelle de la source.

```powershell
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$baseDest        = "_webp"
$extensionSource = "*.png"
$maxThreads      = 12
$qualiteWebp     = 90
$compressionWebp = 4
$LogFile         = "D:\Pix\conversion.log"

$files = Get-ChildItem -Path . -Filter $extensionSource -File
$totalFichiers = $files.Count

if ($totalFichiers -eq 0) { exit }

$RunspacePool = [runspacefactory]::CreateRunspacePool(1, $maxThreads)
$RunspacePool.Open()
$Jobs = [System.Collections.Generic.List[object]]::new()

$index = 0
foreach ($file in $files) {
    $index++
    $partNum = [math]::Floor(($index - 1) / 1000) + 1
    $targetFolder = Join-Path $baseDest "pt_$($partNum.ToString('00'))"
    if (!(Test-Path -LiteralPath $targetFolder)) { New-Item -ItemType Directory -Path $targetFolder | Out-Null }
    $targetPath = Join-Path $targetFolder "$($file.BaseName).webp"

    $PowerShell = [powershell]::Create().AddScript({
        param($src, $dst, $q, $c)
        & ffmpeg -nostdin -i "$src" -vcodec libwebp -pix_fmt yuv420p -color_range 2 `
                 -preset default -q:v $q -compression_level $c -threads 1 -y -loglevel error "$dst"

        if ((Test-Path -LiteralPath $dst) -and ((Get-Item -LiteralPath $dst).Length -gt 0)) {
            Remove-Item -LiteralPath $src -Force
            return [PSCustomObject]@{ Statut = "OK"; Source = $src }
        }
        return [PSCustomObject]@{ Statut = "ERREUR"; Source = $src }
    }).AddArgument($file.FullName).AddArgument($targetPath).AddArgument($qualiteWebp).AddArgument($compressionWebp)

    $PowerShell.RunspacePool = $RunspacePool
    $Jobs.Add([PSCustomObject]@{ Pipe = $PowerShell; Result = $PowerShell.BeginInvoke() })
}

while ($Jobs.Count -gt 0) {
    for ($j = $Jobs.Count - 1; $j -ge 0; $j--) {
        if ($Jobs[$j].Result.IsCompleted) {
            $taskReturn = $Jobs[$j].Pipe.EndInvoke($Jobs[$j].Result)
            $Jobs[$j].Pipe.Dispose()
            $Jobs.RemoveAt($j)
        }
    }
    Start-Sleep -Milliseconds 100
}

$RunspacePool.Close()
$RunspacePool.Dispose()
[System.Console]::Beep(880, 500)
```

---

## 5. Audit d'intégrité & Injection SQL MD5

Calcul en flux direct de l'empreinte MD5 pour insertion dans la table `pwg_images` de Piwigo :

```powershell
$DossierLocal = "C:\Photos\MonAlbum"
$FichierSqlSortie = "C:\Photos\update_md5.sql"

"START TRANSACTION;" | Out-File -FilePath $FichierSqlSortie -Encoding UTF8

$Photos = Get-ChildItem -Path $DossierLocal -Include *.webp, *.jpg, *.png -Recurse -File
$md5 = [System.Security.Cryptography.MD5]::Create()

foreach ($Photo in $Photos) {
    $stream = [System.IO.File]::OpenRead($Photo.FullName)
    $hashBytes = $md5.ComputeHash($stream)
    $stream.Close()
    $stream.Dispose()
    $HashString = (-join ($hashBytes | ForEach-Object { "{0:x2}" -f $_ }))

    $NomFichier = $Photo.Name
    $LigneSql = "UPDATE pwg_images SET md5sum = '$HashString' WHERE file = '$NomFichier' AND (md5sum IS NULL OR md5sum = '');"
    $LigneSql | Out-File -FilePath $FichierSqlSortie -Append -Encoding UTF8
}

"COMMIT;" | Out-File -FilePath $FichierSqlSortie -Append -Encoding UTF8
```

---

## 6. Pré-génération locale des Dérivés (CLI Laragon)

Exécution dans le terminal Laragon pour épargner le processeur d'o2switch :

```bash
# Identifier l'album
php plugins/piwigo-cli/bin/pwg.php album list

# Générer les dérivés en multithread (-p ID, -j workers)
php plugins\piwigo-cli\bin\pwg.php photo generate-derivatives -p 24 -j 12
```

---

## 7. Synchronisation Distante (rclone)

### Déploiement des Dérivés vers o2switch
```bash
rclone copy "D:\Websites\__LARAGON__\www\gallery\_data\i" o2switch.website:gallery.dakta.website/_data/i -P --transfers=8 --checkers=16
```

### Réplication Stockage Objet vers Mega S4
```powershell
# Profils adaptés selon la typologie de fichiers :
# Video  : --transfers=6  --checkers=8  --s3-chunk-size=32M --buffer-size=32M
# Images : --transfers=32 --checkers=64 --s3-chunk-size=5M  --buffer-size=0M
& rclone.exe copy "D:\Videos\Scenes" "megas4:s4-vidz/Ffmpeg" --fast-list --s3-acl=public-read -P
```