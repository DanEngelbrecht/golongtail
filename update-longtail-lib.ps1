param($version)

New-Item -Path .\download-longtaillib-${version} -ItemType Directory -Force

Invoke-WebRequest https://github.com/DanEngelbrecht/longtail/releases/download/${version}/darwin-arm64.zip -OutFile .\download-longtaillib-${version}\darwin-arm64.zip
Invoke-WebRequest https://github.com/DanEngelbrecht/longtail/releases/download/${version}/darwin-x64.zip -OutFile .\download-longtaillib-${version}\darwin-x64.zip
Invoke-WebRequest https://github.com/DanEngelbrecht/longtail/releases/download/${version}/linux-x64.zip -OutFile .\download-longtaillib-${version}\linux-x64.zip
Invoke-WebRequest https://github.com/DanEngelbrecht/longtail/releases/download/${version}/win32-x64.zip -OutFile .\download-longtaillib-${version}\win32-x64.zip

Expand-Archive -Path .\download-longtaillib-${version}\darwin-arm64.zip -DestinationPath .\download-longtaillib-${version}\darwin-arm64 -Force
Expand-Archive -Path .\download-longtaillib-${version}\darwin-x64.zip -DestinationPath .\download-longtaillib-${version}\darwin-x64 -Force
Expand-Archive -Path .\download-longtaillib-${version}\linux-x64.zip -DestinationPath .\download-longtaillib-${version}\linux-x64 -Force
Expand-Archive -Path .\download-longtaillib-${version}\win32-x64.zip -DestinationPath .\download-longtaillib-${version}\win32-x64 -Force

Copy-Item -Path .\download-longtaillib-${version}\darwin-arm64\dist-darwin-arm64\liblongtail_darwin_arm64.a -Destination .\longtaillib\longtail\liblongtail_darwin_arm64.a
Copy-Item -Path .\download-longtaillib-${version}\darwin-x64\dist-darwin-x64\liblongtail_darwin_x64.a -Destination .\longtaillib\longtail\liblongtail_darwin_x64.a
Copy-Item -Path .\download-longtaillib-${version}\linux-x64\dist-linux-x64\liblongtail_linux_x64.a -Destination .\longtaillib\longtail\liblongtail_linux_x64.a
Copy-Item -Path .\download-longtaillib-${version}\win32-x64\dist-win32-x64\liblongtail_win32_x64.a -Destination .\longtaillib\longtail\liblongtail_win32_x64.a

Copy-Item -Path .\download-longtaillib-${version}\win32-x64\dist-win32-x64\include .\longtaillib\longtail\ -Recurse -Force
