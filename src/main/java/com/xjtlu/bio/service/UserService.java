package com.xjtlu.bio.service;

import java.util.List;

import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import com.xjtlu.bio.common.Result;
import com.xjtlu.bio.entity.BioUser;
import com.xjtlu.bio.entity.BioUserExample;
import com.xjtlu.bio.mapper.BioUserMapper;

import jakarta.annotation.Resource;

@Service
public class UserService {

    @Resource
    private BioUserMapper userMapper;

    @Resource
    private PasswordEncoder passwordEncoder;

    public BioUser getUserByName(String username) {
        // 这里应该调用 UserMapper 来从数据库中获取用户信息
        // 例如：return userMapper.selectByUsername(username);
        BioUserExample example = new BioUserExample();
        example.createCriteria().andNameEqualTo(username);
        List<BioUser> users = userMapper.selectByExample(example);
        if (users.isEmpty()) {
            return null;
        } else {
            return users.get(0);
        }
    }

    public Result<Object> userRegister(String username, String password) {
        String insertFailedMsg = "用户注册失败";
        BioUser user = new BioUser();
        user.setName(username);
        String encoded = passwordEncoder.encode(password);
        user.setPassword(encoded);
        int success = userMapper.insertSelective(user);
        if (success <= 0) {
            return new Result<>(Result.INTERNAL_FAIL, null, insertFailedMsg);
        }
        return new Result<>(Result.SUCCESS, null, "");
    }
}
