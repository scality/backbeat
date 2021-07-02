
function stsResponseToXML(stsResponse) {
    const resp = [
        '<AssumeRoleResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">',
        '<AssumeRoleResult>',
        '<Credentials>',
        '<SessionToken>',
        stsResponse.Credentials.SessionToken,
        '</SessionToken>',
        '<SecretAccessKey>',
        stsResponse.Credentials.SecretAccessKey,
        '</SecretAccessKey>',
        '<Expiration>',
        new Date(stsResponse.Credentials.Expiration).toISOString(),
        '</Expiration>',
        '<AccessKeyId>',
        stsResponse.Credentials.AccessKeyId,
        '</AccessKeyId>',
        '</Credentials>',
        '</AssumeRoleResult>',
        '</AssumeRoleResponse>',
    ];
    return resp.join('');
}

module.exports = {
    stsResponseToXML,
};
