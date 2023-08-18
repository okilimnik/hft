use teloxide::{prelude::*, utils::command::BotCommands};

pub async fn start_bot() {
    let bot = Bot::from_env();
    Command::repl(bot, answer).await;
}

#[derive(BotCommands, Clone)]
#[command(rename_rule = "lowercase", description = "These commands are supported:")]
enum Command {
    #[command(description = "Notifies about gainers in the weekdays mornings")]
    Notify,
}

async fn answer(bot: Bot, msg: Message, cmd: Command) -> ResponseResult<()> {
    match cmd {
        Command::Notify => {
            bot.send_message(msg.chat.id, format!("Done! Your chat ID is: {}", msg.chat.id)).await?
        }
    };

    Ok(())
}