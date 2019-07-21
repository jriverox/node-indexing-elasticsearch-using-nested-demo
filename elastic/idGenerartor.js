let getIdForStrategy = (campaign, cuv, personalizationType) => {
    return `${campaign}.${cuv}.${personalizationType}`;
}

let getIdForPersonalization = (campaign, cuv, personalizationType, consultant, startDay) => {
    return `${campaign}.${cuv}.${personalizationType}.${consultant}.${startDay}`;
}

module.exports = {
    getIdForStrategy,
    getIdForPersonalization
}