#!/bin/bash
# EnergyPulse 版本管理器

VERSION_FILE="config/versions/VERSION"
BETA_HISTORY="config/versions/beta_history.txt"

# 读取当前版本
get_prod_version() {
    grep "PROD_VERSION=" $VERSION_FILE | cut -d= -f2
}

get_beta_version() {
    grep "BETA_VERSION=" $VERSION_FILE | cut -d= -f2
}

# 递增beta版本号
increment_beta() {
    current=$(get_beta_version)
    # 从 beta-X.Y.Z 提取数字
    if [[ $current =~ beta-([0-9]+)\.([0-9]+)\.([0-9]+) ]]; then
        major=${BASH_REMATCH[1]}
        minor=${BASH_REMATCH[2]}
        patch=${BASH_REMATCH[3]}
        new_patch=$((patch + 1))
        new_version="beta-${major}.${minor}.${new_patch}"
    else
        new_version="beta-1.0.0"
    fi
    
    # 记录历史
    echo "$(date): $current -> $new_version" >> $BETA_HISTORY
    
    # 更新版本文件
    sed -i "s/BETA_VERSION=.*/BETA_VERSION=$new_version/" $VERSION_FILE
    echo $new_version
}

# 发布正式版本 (从beta晋升)
promote_to_prod() {
    beta_ver=$(get_beta_version)
    # 去掉beta-前缀
    prod_ver=${beta_ver#beta-}
    
    sed -i "s/PROD_VERSION=.*/PROD_VERSION=$prod_ver/" $VERSION_FILE
    echo $prod_ver
}

case "$1" in
    "get-beta")
        get_beta_version
        ;;
    "get-prod")
        get_prod_version
        ;;
    "increment-beta")
        increment_beta
        ;;
    "promote")
        promote_to_prod
        ;;
    *)
        echo "用法: $0 {get-beta|get-prod|increment-beta|promote}"
        ;;
esac
