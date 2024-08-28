# Contributing

## Prerequisites

```bash
sudo apt update
sudo apt -y upgrade
sudo apt install build-essential libreadline-dev unzip
# Install Lua
curl -L -O http://www.lua.org/ftp/lua-5.3.5.tar.gz
tar -zxf lua-5.3.5.tar.gz
cd lua-5.3.5
make linux test
sudo make install
lua -v # Lua 5.3.5
# Install LuaRocks
curl -R -O https://luarocks.github.io/luarocks/releases/luarocks-3.11.1.tar.gz
tar -zxf luarocks-3.11.1.tar.gz
cd luarocks-3.11.1
./configure --with-lua-include=/usr/local/include
make
sudo make install
luarocks --version # 3.11.1
export PATH="$PATH:$HOME/.luarocks/bin" # Add to .bashrc
# Install Lua modules
luarocks install --local cyan
luarocks install --local amalg
luarocks install --local busted
```
